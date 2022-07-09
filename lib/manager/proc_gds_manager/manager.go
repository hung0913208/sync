package proc_gds_manager

import (
    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/kafka"
    "alpaca.vn/libra/devops/sync/lib/db"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"

    "encoding/json"
    "strings"
    "time"
    "fmt"
)

type MonitorCachingHandler func(batch []interface{})
type MonitorCommitingHandler func(gds db.GdsMessage, isRevert bool, query string)

type ProcessGdsManager interface {
    EnableTesting()
    DisableTesting()
    Monitor(
        cache MonitorCachingHandler, 
        commit MonitorCommitingHandler)
    Flush()
    Close()
}

type ProductionParams struct {
    Group               string
    Topics              []string
    Brokers             []string
    ConsumerTimeout     time.Duration
    ProducerTimeout     time.Duration
}

type StagingParams struct {
    NumPartitions       int
    ReplicationFactor   int
    Brokers             []string
    ProductionTopic     string
    StagingTopic        string
    Group               string
    ConsumerTimeout     time.Duration
    ProducerTimeout     time.Duration
}

type packet struct {
    Dsn     int             `json:"dsn"`
    Gds     db.GdsMessage   `json:"data"`
}

type implProcessGdsManager struct {
    syncFromStaging             sync_gds_manager.SynchronizeGdsManager
    publishFromProduction       kafka.Producer
    subscribeFromCache          kafka.Consumer
    subscribeFromProduction     kafka.Consumer
    dbConnList                  []*gorm.DB
    mapOfTables                 []map[string][]string
    mapOfSchema                 map[string]int
    isTesting                   bool
    monitorCachingHandlers      []MonitorCachingHandler
    monitorCommitingHandlers    []MonitorCommitingHandler
}

func (self *implProcessGdsManager) EnableTesting() {
    self.startSyncingGdsToDb()

    if !self.isTesting {
        self.subscribeFromProduction.Lock()
        self.subscribeFromCache.Lock()
        defer self.subscribeFromCache.Unlock()
        defer self.subscribeFromProduction.Unlock()

        self.isTesting = true
    }
}

func (self *implProcessGdsManager) DisableTesting() {
    self.startSyncingGdsToDb()

    if self.isTesting {
        self.subscribeFromProduction.Lock()
        self.subscribeFromCache.Lock()
        defer self.subscribeFromCache.Unlock()
        defer self.subscribeFromProduction.Unlock()

        self.isTesting = false
    }
}

func (self *implProcessGdsManager) Monitor(
    cache MonitorCachingHandler, 
    commit MonitorCommitingHandler,
) {
    self.monitorCachingHandlers = append(self.monitorCachingHandlers, cache)
    self.monitorCommitingHandlers = append(self.monitorCommitingHandlers, commit)
}

func (self *implProcessGdsManager) Flush() { 
    self.publishFromProduction.Flush()
}

func (self *implProcessGdsManager) Close() {
    self.publishFromProduction.Close()
    self.subscribeFromCache.Close()
    self.subscribeFromProduction.Close()
}

func (self *implProcessGdsManager) startSyncingGdsToDb() {
    self.subscribeFromProduction.Subscribe(
        sync_gds_manager.PostgresNotifierKey,
        self.syncGdsToPostgresDb)

    self.subscribeFromCache.Subscribe(
        sync_gds_manager.PostgresNotifierKey,
        self.syncProductionCacheToDb)
}

func (self *implProcessGdsManager) syncProductionCacheToDb(batch []interface{}) error {
    if !self.isTesting {
        return self.handleBatchGdsMessage(batch, false)
    }

    return nil
}

func (self *implProcessGdsManager) revertTestingRecordFromStagingDb(batch []interface{}) error {
    if !self.isTesting {
        return self.handleBatchGdsMessage(batch, true)
    }

    return nil
}

func (self *implProcessGdsManager) syncGdsToPostgresDb(batch []interface{}) error {
    if self.isTesting {
        self.onCaching(batch)

        return self.publishFromProduction.PublishBatchWithSameKey(
            sync_gds_manager.PostgresNotifierKey,
            batch)
    } else {
        return self.handleBatchGdsMessage(batch, false)
    }
}

func (self *implProcessGdsManager) handleBatchGdsMessage(
    batch []interface{},
    revert bool,
) error {
    for _, msg := range batch { 
        var pkt packet

        if err := json.Unmarshal([]byte(msg.(string)), &pkt); err != nil {
            return err 
        }

        // @TODO: how to handle error, do we need to push it to log center
        err := self.handleGdsMessage(self.dbConnList[pkt.Dsn], pkt.Gds, revert)
        if err != nil {
            return err
        }
    }

    return nil
}

func (self *implProcessGdsManager) handleGdsMessage(
    dbConn *gorm.DB,
    msg db.GdsMessage,
    revert bool,
) error {
    var query string

    switch msg.Action {
    case "INSERT":
        columns := make([]string, 0)
        values := make([]string, 0)

        for columnName, value := range msg.New {
            columns = append(columns, columnName)

            if _, ok := value.(string); ok {
                values = append(values, fmt.Sprintf("\"%s\"", value))
            } else {
                values = append(values, fmt.Sprint(value))
            }
        }

        query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);",
                            msg.Schema,
                            msg.Table,
                            strings.Join(columns, ", "),
                            strings.Join(values, ", "))

    case "UPDATE":
        templateOfIdName := []string{"id", "ID", "Id", "iD"}
        condition := ""
        columns := make([]string, 0)
        values := make([]string, 0)
        recordOld := msg.Old
        recordNew := msg.New

        if revert {
            recordOld = msg.New
            recordNew = msg.Old
        }

        for _, possibleName := range templateOfIdName { 
            if value, ok := recordOld[possibleName]; ok {
                data := ""

                if _, ok := value.(string); ok {
                    data = fmt.Sprintf("\"%s\"", value)
                } else {
                    data = fmt.Sprint(value)
                }

                condition = fmt.Sprintf("%s = %s", possibleName, data)
                break
            } 
        }

        // @TODO: make revert command
        if len(condition) == 0 {
            fields := make([]string, 0)
            data := make([]string, 0)

            for columnName, value := range recordOld {
                for _, possibleName := range templateOfIdName {
                    if strings.Contains(columnName, possibleName) {
                        fields = append(fields, columnName)
            
                        if _, ok := value.(string); ok {
                            data = append(data, fmt.Sprintf("\"%s\"", value))
                        } else {
                            data = append(data, fmt.Sprint(value))
                        }
                    }
                }
            }

            for i := 0; i < len(fields); i++ {
                item := fmt.Sprintf("%s = %s", fields[i], values[i])

                if len(condition) > 0 {
                    condition = fmt.Sprintf("%s AND %s", condition, item)
                } else {
                    condition = item
                }
            }
        }

        for columnName, value := range recordNew {
            columns = append(columns, columnName)
            
            if _, ok := value.(string); ok {
                values = append(values, fmt.Sprintf("\"%s\"", value))
            } else {
                values = append(values, fmt.Sprint(value))
            }
        }

        query = fmt.Sprintf("UPDATE %s.%s SET (%s) = (%s) WHERE %s;",
                            msg.Schema,
                            msg.Table,
                            strings.Join(columns, ", "),
                            strings.Join(values, ", "),
                            condition)

    default:
        return nil
    }

    self.onCommiting(msg, revert, query)

    tx := dbConn.Exec(query)
    return tx.Error
}

func (self *implProcessGdsManager) onCaching(batch []interface{}) {
    for _, handler := range self.monitorCachingHandlers {
        handler(batch)
    }
}

func (self *implProcessGdsManager) onCommiting(
    msg db.GdsMessage,
    revert bool,
    query string,
) {
    for _, handler := range self.monitorCommitingHandlers {
        handler(msg, revert, query)
    }
}

func NewProcessGdsManager(
    prodParams ProductionParams,
    stagParams StagingParams,
    dsnList    []string,
) (ProcessGdsManager, error) {
    dbConnList := make([]*gorm.DB, 0)

    for _, dsn := range dsnList {
        dbConn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
        if err != nil {
            return nil, err
        }

        db, err := dbConn.DB()
        if err != nil {
            return nil, err
        }

        db.SetMaxIdleConns(10)
        db.SetMaxOpenConns(100)
        db.SetConnMaxLifetime(time.Hour)

        dbConnList = append(dbConnList, dbConn)
    }

    pushProductionToCacheProducer, err := kafka.NewProducer( 
        stagParams.NumPartitions,
        stagParams.ReplicationFactor,
        stagParams.Brokers,
        stagParams.ProductionTopic,
        stagParams.ProducerTimeout)
    if err != nil {
        return nil, err
    }

    pushProductionToDbConsumer, err := kafka.NewConsumer(
        prodParams.Group, 
        prodParams.Topics, 
        prodParams.Brokers, 
        prodParams.ConsumerTimeout)
    if err != nil {
        return nil, err
    }

    pushCacheToDbConsumer, err := kafka.NewConsumer(
        stagParams.Group, 
        []string{stagParams.StagingTopic},
        stagParams.Brokers,
        stagParams.ConsumerTimeout)
    if err != nil {
        return nil, err
    }

    stagingSyncManager, err := sync_gds_manager.NewSynchronizeGdsManager(
        dsnList,
        stagParams.NumPartitions,
        stagParams.ReplicationFactor,
        stagParams.Brokers,
        stagParams.StagingTopic,
        stagParams.ProducerTimeout)
    if err != nil {
        return nil, err
    }

    return &implProcessGdsManager{
        dbConnList:              dbConnList,
        syncFromStaging:         stagingSyncManager,
        publishFromProduction:   pushProductionToCacheProducer,
        subscribeFromCache:      pushCacheToDbConsumer,
        subscribeFromProduction: pushProductionToDbConsumer,
    }, nil
}

