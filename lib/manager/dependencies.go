package manager

import (
    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/manager/proc_gds_manager"
)

type Dependencies struct {
    syncGdsManager sync_gds_manager.SynchronizeGdsManager
    processGdsManager proc_gds_manager.ProcessGdsManager
}

func NewDependencies(isStaging bool) (*Dependencies, error) {
    // @TODO: we must find a way to dynamic configure the whole system without
    //        rebuild code again

    var procGdsManager proc_gds_manager.ProcessGdsManager
    var syncGdsManager sync_gds_manager.SynchronizeGdsManager
    var err error

    if isStaging {
        procGdsManager, err = proc_gds_manager.NewProcessGdsManager(
            proc_gds_manager.ProductionParams{
                Group:   ProductionKafkaGroup,
                Topics:  ProductionKafkaBrokerList,
                Brokers: ProductionKafkaBrokerList,
                Timeout: GdsCommonTimeout,
            },
            proc_gds_manager.StagingParams{
                NumPartitions:      KafkaNumPartitions,
                ReplicationFactor:  KafkaReeplicationFactor,
                Brokers:            StagingKafkaBrokerList,
                ProductionTopic:    ProductionGdsKafkaTopic,
                StagingTopic:       StagingGdsKafkaTopic,
                Group:              StagingKafkaGroup,
                Timeout:            GdsCommonTimeout,
            },
            StagingPostgresDsnList)
    } else {
        syncGdsManager, err = sync_gds_manager.NewSynchronizeGdsManager(
            ProductionPostgresDsnList,
            KafkaNumPartitions, KafkaReeplicationFactor,
            ProductionKafkaBrokerList,
            ProductionGdsKafkaTopic,
            GdsCommonTimeout)
        if err != nil {
            return nil, err
        }
    }

    return &Dependencies{
        syncGdsManager: syncGdsManager,
        processGdsManager: procGdsManager,
    }, nil
}
