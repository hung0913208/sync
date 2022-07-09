package main

import (
    "path/filepath"
    "io/ioutil"
    "testing"
    "syscall"
    "time"
    "fmt"
    "os"

    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/manager/proc_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/manager"
    "alpaca.vn/libra/devops/sync/lib/db"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)


type SyncGdsTestSuite struct {
    suite.Suite
    db *gorm.DB
}

func TestSyncGds(t *testing.T) {
    suite.Run(t, new(SyncGdsTestSuite))
}

type TestSync struct {
    Name    string
}

func (self *TestSync) TableName() string {
    return "test_tab"
}


func (suite *SyncGdsTestSuite) SetupSuite() {
    dsn := "host=production-postgres user=postgres password=postgres port=5432 sslmode=disable"

    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
    if err != nil {
        panic(err)
    }

    db.Exec("CREATE DATABASE test_db;")

    dsn = "host=production-postgres user=postgres password=postgres dbname=test_db port=5432 sslmode=disable"
    cnt := 0
    err = filepath.Walk("../fixture", func(path string, f os.FileInfo, err error) error {
        if filepath.Ext(path) == ".sql" {
            query, err := ioutil.ReadFile(path)
            if err != nil {
                panic(err)
            }

            tx := db.Exec(string(query))
            if tx.Error != nil {
                panic(tx.Error)
            }

            cnt += 1
        }

        return nil
    })

    if err != nil {
        panic(err)
    }

    if cnt == 0 {
        panic("can't perform fixture")
    }

    suite.db = db
}

func (suite *SyncGdsTestSuite) TestSyncGdsToKafka() {
    cntSyncGdsToKafka := 0
    cntCommitGdsToDb := 0
    cntCachingGds := 0

    syncGdsManager, err := sync_gds_manager.NewSynchronizeGdsManager(
        manager.ProductionPostgresDsnList,
        manager.KafkaNumPartitions,
        manager.KafkaReeplicationFactor,
        manager.ProductionKafkaBrokerList,
        manager.ProductionGdsKafkaTopic,
        manager.GdsCommonTimeout)
    assert.Nil(suite.T(), err)

    procGdsManager, err := proc_gds_manager.NewProcessGdsManager(
        proc_gds_manager.ProductionParams{
            Group:              manager.ProductionKafkaGroup,
            Topics:             []string{manager.ProductionGdsKafkaTopic},
            Brokers:            manager.ProductionKafkaBrokerList,
            ProducerTimeout:    manager.GdsCommonTimeout,
            ConsumerTimeout:    manager.GdsConsumeTimeout,
        },
        proc_gds_manager.StagingParams{
            NumPartitions:      manager.KafkaNumPartitions,
            ReplicationFactor:  manager.KafkaReeplicationFactor,
            Brokers:            manager.StagingKafkaBrokerList,
            ProductionTopic:    manager.ProductionGdsKafkaTopic,
            StagingTopic:       manager.StagingGdsKafkaTopic,
            Group:              manager.StagingKafkaGroup,
            ProducerTimeout:    manager.GdsCommonTimeout,
            ConsumerTimeout:    manager.GdsConsumeTimeout,
        },
        manager.StagingPostgresDsnList)
    assert.Nil(suite.T(), err)

    err = syncGdsManager.RegisterTable(
        sync_gds_manager.PostgresNotifierKey,
        0,
        "test_tab")
    assert.Nil(suite.T(), err)

    syncGdsManager.Monitor(
        sync_gds_manager.PostgresNotifierKey,
        func(id int, msg string) {
            assert.Equal(suite.T(), 0, id)
            cntSyncGdsToKafka += 1
        },
    )

    procGdsManager.Monitor(
        func (batch []interface{}) {
            fmt.Println("batch", batch)
            cntCachingGds += 1
        },
        func (gds db.GdsMessage, isRevert bool, query string) {
            fmt.Println("query", query)
            cntCommitGdsToDb += 1
        },
    )

    time.Sleep(100000)
    procGdsManager.DisableTesting()

    go func() {
        syncGdsManager.HandleSyncGdsToKafka(time.Minute)
    }()

    result := suite.db.Create(&Test{Name: "test"})
    assert.Nil(suite.T(), result.Error)

    syncGdsManager.Flush()
    procGdsManager.Flush()
    time.Sleep(100000)

    assert.Equal(suite.T(), 1, cntSyncGdsToKafka)
    assert.Equal(suite.T(), 1, cntCommitGdsToDb)
    
    //procGdsManager.EnableTesting()
    //time.Sleep(50000)

    //result = suite.db.Create(&Test{Name: "test"})
    //assert.Nil(suite.T(), result.Error)

    //syncGdsManager.Flush()
    //procGdsManager.Flush()
    //time.Sleep(50000)

    //procGdsManager.DisableTesting()
    //time.Sleep(10)

    //syncGdsManager.Flush()
    //procGdsManager.Flush()
    //time.Sleep(50000)

    //assert.Equal(suite.T(), 2, cntSyncGdsToKafka)
    //assert.Equal(suite.T(), 1, cntCachingGds)
    //assert.Equal(suite.T(), 2, cntCommitGdsToDb)

    syscall.Kill(syscall.Getpid(), syscall.SIGINT) 
    time.Sleep(100000)
}

