package main

import (
    "path/filepath"
    "io/ioutil"
    "testing"
    "time"
    "fmt"
    "os"

    "alpaca.vn/libra/devops/sync/lib/notify"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"

    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)

type PostgresTestSuite struct {
    suite.Suite
    notifier    notify.Notifier
    db          *gorm.DB
}

type Test struct {
    gorm.Model
    Id      int     `gorm:"primaryKey"`
    Name    string
}

func (self *Test) TableName() string {
    return "test_tab"
}

func (suite *PostgresTestSuite) SetupTest() {
    dsn := "host=postgres user=postgres password=postgres port=5432 sslmode=disable"
    cnt := 0

    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
    if err != nil {
        panic(err)
    }

    tx := db.Exec("DROP DATABASE IF EXISTS test_db;")
    if tx.Error != nil {
        panic(tx.Error)
    }

    tx = db.Exec("CREATE DATABASE test_db;")
    if tx.Error != nil {
        panic(tx.Error)
    }

    dsn = "host=postgres user=postgres password=postgres dbname=test_db port=5432 sslmode=disable"

    db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{}) 
    if err != nil {
        panic(err)
    }

    err = filepath.Walk("./fixture", func(path string, f os.FileInfo, err error) error {
        query, err := ioutil.ReadFile(path)
        if err != nil {
            panic(err)
        }

        tx := db.Exec(string(query))
        if tx.Error != nil {
            panic(tx.Error)
        }

        cnt += 1
        return nil
    })

    if cnt == 0 {
        panic("can't perform fixture")
    }

    notifier, err := notify.NewPostgresNotifier(dsn) 
    if err != nil {
        panic(err)
    }

    suite.notifier = notifier
    suite.db = db
}

func TestPostgres(t *testing.T) {
    suite.Run(t, new(PostgresTestSuite))
}

func (suite *PostgresTestSuite) TestCatchingInsertRequest() {
    cnt := 0

    result := suite.db.Create(&Test{Id: 1, Name: "test"})
    assert.Nil(suite.T(), result.Error)

    suite.notifier.Notify(func(msg string) error {
        cnt += 1
        fmt.Println(msg)
        return nil
    },
    time.Second)

    assert.Equal(suite.T(), cnt, 1)
}
