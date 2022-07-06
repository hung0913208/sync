package kafka

import (
    "github.com/confluentinc/confluent-kafka-go/kafka"

    "encoding/json"
    "context"
    "strings"
    "errors"
    "time"
)


type implProducerClient struct {
    producer *kafka.Producer
    topic    string
    timeout  time.Duration
}

func (self *implProducerClient) PublishMessage(key string, msg interface{}) error {
    return self.PublishBatchWithSameKey(key, []interface{}{msg})
}

func (self *implProducerClient) PublishBatchMultipleKey(keys []string, batch []interface{}) error {
    mapOfKeyBatch := make(map[string][]interface{})

    if len(keys) != len(batch) {
        return errors.New("len(keys) != len(batch)")
    }

    for i, key := range keys {
        if _, ok := mapOfKeyBatch[key]; !ok {
            mapOfKeyBatch[key] = make([]interface{}, 0)
        }
        
        mapOfKeyBatch[key] = append(mapOfKeyBatch[key], batch[i])
    }

    for key, batch := range mapOfKeyBatch {
        err := self.PublishBatchWithSameKey(key, batch)
        if err != nil {
            return err
        }
    }

    return nil
}

func (self *implProducerClient) PublishBatchWithSameKey(key string, batch []interface{}) error {
    marshaled, err := json.Marshal(&batch)
    if err != nil {
        return err
    }

    self.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic:      &self.topic,
            Partition:  kafka.PartitionAny,
        },
        Key:   []byte(key),
        Value: []byte(marshaled),
    }, nil)

    return nil
}

func (self *implProducerClient) Flush() {
    self.producer.Flush(int(self.timeout / time.Millisecond))
}

func (self *implProducerClient) Close() {
    self.Flush()
    self.producer.Close()
}

func createTopic(
    producer *kafka.Producer,
    topic string, 
    numPartitions, replicationFactor int,
    timeout time.Duration,
) error {
	admin, err := kafka.NewAdminClientFromProducer(producer)
	if err != nil {
        return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer admin.Close()

	results, err := admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor}},
		kafka.SetAdminOperationTimeout(timeout))
	if err != nil {
        return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
            return result.Error
		}
	}

    return nil
}

func NewProducer(
    numPartitions, replicationFactor int,
    brokers []string, 
    topic string,
    timeout time.Duration,
) (Producer, error) {
    stringListOfBrokers := strings.Join(brokers[:], ",")
    producer, err := kafka.NewProducer(
        &kafka.ConfigMap{"bootstrap.servers": stringListOfBrokers},
    )
    if err != nil {
        return nil, err
    }
    
    err = createTopic(producer, topic, numPartitions, replicationFactor, timeout)
    if err != nil {
        producer.Close()
        return nil, err
    }

    return &implProducerClient{
        producer: producer,
        topic:    topic,
        timeout:  timeout,
    }, nil
}
