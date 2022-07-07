package kafka

import (
    "github.com/confluentinc/confluent-kafka-go/kafka"

    "encoding/json"
    "strings"
    "time"
    "sync"
    "fmt"
)

type implConsumerClient struct {
    consumer    *kafka.Consumer
    timeout     time.Duration
    handlers    map[string]Handler
    mutex       sync.Mutex
    topics      []string
    started     bool
    closing     chan bool
    closed      chan bool
}

func (self *implConsumerClient) Subscribe(key string, handler Handler) error {
    self.mutex.Lock()
    self.handlers[key] = handler

    if self.started == false {
        self.started = true
        self.mutex.Unlock()

        err := self.consumer.SubscribeTopics(self.topics, nil)
        if err != nil {
            return err
        }

        go func() {
            for {
                select {
                case <- self.closing:
                    self.closed <- true
                    return
                default:
                    msg, err := self.consumer.ReadMessage(self.timeout)
                    if err != nil {
                        continue
                    }

                    key := string(msg.Key)
                    value := msg.Value
                    batch := []interface{}{}

                    self.mutex.Lock()
                    handler, ok := self.handlers[key]
                    self.mutex.Unlock()

                    if ok {
                        err = json.Unmarshal(value, &batch)
                        if err != nil {
                            self.HandleError(err)
                            continue
                        }

                        err = handler(batch)
                        if err != nil {
                            self.HandleError(err)
                        }
                    } else {
                        fmt.Println(key)
                    }
                }
            }
        }() 
    } else {
        self.mutex.Unlock()
    }

    return nil
}
    
func (self *implConsumerClient) HandleError(err error) {
    fmt.Println("error", err)
}

func (self *implConsumerClient) Close() {
    self.closing <- true

    <-self.closed
        
    self.mutex.Lock()
    self.started = false
    self.mutex.Unlock()
    self.consumer.Close()
}

func NewConsumer(
    group string,
    topics, brokers []string,
    timeout time.Duration,
) (Consumer, error) {
    stringListOfBrokers := strings.Join(brokers[:], ",")
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": stringListOfBrokers,
        "group.id":          group,
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        return nil, err
    }

    return &implConsumerClient{
        consumer:   consumer,
        timeout:    timeout,
        handlers:   make(map[string]Handler),
        topics:     topics,
        started:    false,
        closing:    make(chan bool, 1),
        closed:     make(chan bool, 1),
    }, nil
}
