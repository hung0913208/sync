package main

import (
    "path/filepath"
    "io/ioutil"
    "testing"
    "syscall"
    "time"
    "os"

    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/manager"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)


type SyncGdsTestSuite struct {
    suite.Suite
    db          *gorm.DB
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
    dsn := "host=postgres user=postgres password=postgres port=5432 sslmode=disable"

    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
    if err != nil {
        panic(err)
    }

    db.Exec("CREATE DATABASE test_db;")

    dsn = "host=postgres user=postgres password=postgres dbname=test_db port=5432 sslmode=disable"
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
    cnt := 0
    syncGdsManager, err := sync_gds_manager.NewSynchronizeGdsManager(
        manager.PostgresDsnList,
        manager.KafkaNumPartitions,
        manager.KafkaReeplicationFactor,
        manager.KafkaBrokerList,
        manager.GdsKafkaTopic,
        manager.GdsCommonTimeout)
    assert.Nil(suite.T(), err)

    err = syncGdsManager.RegisterTable(
        sync_gds_manager.PostgresNotifierKey,
        0,
        "test_tab")
    assert.Nil(suite.T(), err)

    syncGdsManager.RegisterMonitor(
        manager.PostgresNotifierKey,
        func(id int, msg string) {
            assert.Equal(suite.T(), 0, id)
            cnt += 1
        },
    )

    go func() {
        syncGdsManager.HandleSyncGdsToKafka(time.Minute)
    }()

    result := suite.db.Create(&Test{Name: "test"})
    assert.Nil(suite.T(), result.Error)

    syscall.Kill(syscall.Getpid(), syscall.SIGINT) 
    time.Sleep(10)

    assert.Equal(suite.T(), 1, cnt)
}
