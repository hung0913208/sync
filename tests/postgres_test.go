package main

import (
    "path/filepath"
    "io/ioutil"
    "testing"
    "time"
    "sync"
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
    Name    string
}

func (self *Test) TableName() string {
    return "test_tab"
}

func (suite *PostgresTestSuite) SetupTest() {
    dsn := "host=postgres user=postgres password=postgres port=5432 sslmode=disable"

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
    wg := &sync.WaitGroup{}
    cnt := 0

    wg.Add(1)

    go func() {
        defer wg.Done()

        suite.notifier.Notify(func(msg string) error {
            cnt += 1
            fmt.Println(msg)
            return nil
        },
        60 * time.Second)
    }()

    err := suite.notifier.Register("test_tab")
    assert.Nil(suite.T(), err)

    result := suite.db.Create(&Test{Name: "test"})
    assert.Nil(suite.T(), result.Error)

    wg.Wait()
    assert.Equal(suite.T(), cnt, 1)
}

func (suite *PostgresTestSuite) TestMultiplePushing() {
    wg := &sync.WaitGroup{}
    cnt := 0

    wg.Add(1)

    go func() {
        defer wg.Done()

        suite.notifier.Notify(func(msg string) error {
            cnt += 1
            fmt.Println(msg)
            return nil
        },
        60 * time.Second)
    }()

    err := suite.notifier.Register("test_tab")
    assert.Nil(suite.T(), err)

    for i := 0; i < 1000; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            result := suite.db.Create(&Test{Name: "test"})
            assert.Nil(suite.T(), result.Error)
        }()
    }

    wg.Wait()
    assert.Equal(suite.T(), cnt, 1000)
}
