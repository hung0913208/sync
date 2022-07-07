package proc_gds_manager

import (
    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/kafka"
    "alpaca.vn/libra/devops/sync/lib/db"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"

    "strings"
    "errors"
    "time"
    "fmt"
)

type ProcessGdsManager interface {
    EnableTesting()
    DisableTesting()
}

type ProductionParams struct {
    Group   string
    Topics  []string
    Brokers []string
    Timeout time.Duration
}

type StagingParams struct {
    NumPartitions       int
    ReplicationFactor   int
    Brokers             []string
    ProductionTopic     string
    StagingTopic        string
    Group               string
    Timeout             time.Duration
}

type implProcessGdsManager struct {
    syncFromStaging             sync_gds_manager.SynchronizeGdsManager
    publishFromProduction       kafka.Producer
    subscribeFromCache          kafka.Consumer
    subscribeFromProduction     kafka.Consumer
    dbConn                      *gorm.DB
    isTesting                   bool
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

func (self *implProcessGdsManager) startSyncingGdsToDb() {
    self.subscribeFromProduction.Subscribe(
        PostgresNotifierKey,
        self.syncGdsToPostgresDb)

    self.subscribeFromCache.Subscribe(
        PostgresNotifierKey,
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
        // @TODO: how to handle error, do we need to push it to log center

        return self.publishFromProduction.PublishBatchWithSameKey(
            PostgresNotifierKey,
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
        gdsMsg, ok := msg.(db.GdsMessage)
        if !ok {
            return errors.New("can't parse gds message")
        }

        // @TODO: how to handle error, do we need to push it to log center
        err := self.handleGdsMessage(gdsMsg, revert)
        if err != nil {
            return err
        }
    }

    return nil
}

func (self *implProcessGdsManager) handleGdsMessage(msg db.GdsMessage, revert bool) error {
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
        tx := self.dbConn.Exec(query)
        return tx.Error

    case "UPDATE":
        templateOfIdName := []string{"id", "ID", "Id", "iD"}
        columns := make([]string, 0)
        values := make([]string, 0)
        condition := ""

        for _, possibleName := range templateOfIdName {
            if _, ok := msg.Old[possibleName]; ok {
                condition = fmt.Sprintf("")
                break
            } 
        }

        // @TODO: make revert command
        if len(condition) == 0 {
            fields := make([]string, 0)
            data := make([]string, 0)

            for columnName, value := range msg.Old {
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


        for columnName, value := range msg.New {
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

    // @TODO: log query to log center for debuging
    tx := self.dbConn.Exec(query)
    return tx.Error

}

func NewProcessGdsManager(
    prodParams ProductionParams,
    stagParams StagingParams,
    dsn        string,
) (ProcessGdsManager, error) {
    dbConn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
    if err != nil {
        return nil, err
    }

    productionProducer, err := kafka.NewProducer( 
        stagParams.NumPartitions,
        stagParams.ReplicationFactor,
        stagParams.Brokers,
        stagParams.ProductionTopic,
        stagParams.Timeout)
    if err != nil {
        return nil, err
    }

    productionConsumer, err := kafka.NewConsumer(
        prodParams.Group, 
        prodParams.Topics, 
        prodParams.Brokers, 
        prodParams.Timeout)
    if err != nil {
        return nil, err
    }

    cacheConsumer, err := kafka.NewConsumer(
        stagParams.Group, 
        []string{stagParams.ProductionTopic},
        stagParams.Brokers,
        stagParams.Timeout)
    if err != nil {
        return nil, err
    }

    // @TODO: change the way to handle multiple dsn
    stagingSyncManager, err := sync_gds_manager.NewSynchronizeGdsManager(
        []string{dsn},
        stagParams.NumPartitions,
        stagParams.ReplicationFactor,
        stagParams.Brokers,
        stagParams.StagingTopic,
        stagParams.Timeout)
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

    return &implProcessGdsManager{
        dbConn:                  dbConn,
        syncFromStaging:         stagingSyncManager,
        publishFromProduction:   productionProducer,
        subscribeFromCache:      cacheConsumer,
        subscribeFromProduction: productionConsumer,
    }, nil
}

