package main

import (
    "testing"
    "time"
    "fmt"

    "alpaca.vn/libra/devops/sync/lib/kafka"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"
)


type KafkaTestSuite struct {
    suite.Suite
}

func TestKafka(t *testing.T) {
    suite.Run(t, new(KafkaTestSuite))
}

func (suite *KafkaTestSuite) TestPubSub() {
    cnt := 0
    p, err := kafka.NewProducer(
        1, 1, 
        []string{"kafka:9092"},
        "gds",
        time.Minute)
    assert.Nil(suite.T(), err)
    defer p.Close()

    c, err := kafka.NewConsumer(
        "test1",
        []string{"gds"},
        []string{"kafka:9092"},
        100 * time.Millisecond)
    assert.Nil(suite.T(), err)
    defer c.Close()

    c.Subscribe("postgres", func(batch []interface{}) error {
        fmt.Println(batch)
        cnt += 1
        return nil
    })

    p.PublishBatchWithSameKey("postgres", []interface{}{
        map[string]string{"id": "1", "name": "a"},
        map[string]string{"id": "2", "name": "b"},
    })
    p.Flush()
    
    time.Sleep(30)
    assert.Equal(suite.T(), 1, cnt)
}
