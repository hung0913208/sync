package manager

import (
    "time"
)

const (
    PostgresNotifierKey     = "postgres"
    KafkaNumPartitions      = 1
    KafkaReeplicationFactor = 1
    ProductionGdsKafkaTopic = "gds-production"
    StagingGdsKafkaTopic    = "gds-staging"
    ProductionKafkaGroup    = "gds-production"
    StagingKafkaGroup       = "gds-staging"
    GdsCommonTimeout        = time.Second
    GdsConsumeTimeout       = 100 * time.Millisecond
)

var (
    ProductionPostgresDsnList = []string{        
        "host=production-postgres user=postgres password=postgres port=5432 sslmode=disable",
    }
    StagingPostgresDsnList = []string{
        "host=staging-postgres user=postgres password=postgres port=5432 sslmode=disable",
    }
    ProductionKafkaBrokerList = []string{
        "production-kafka:9092",
    }
    StagingKafkaBrokerList = []string{
        "staging-kafka:9092",
    }
)
