package manager

import (
    "time"
)

const (
    PostgresNotifierKey     = "postgres"
    KafkaNumPartitions      = 1
    KafkaReeplicationFactor = 1
    GdsKafkaTopic           = "gds"
    GdsCommonTimeout        = 30 * time.Second
)

var (
    PostgresDsnList = []string{        
        "host=postgres user=postgres password=postgres port=5432 sslmode=disable",
    }
    KafkaBrokerList = []string{
        "kafka:9092",
    }
)
