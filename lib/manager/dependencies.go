package manager

import (
    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
    "alpaca.vn/libra/devops/sync/lib/manager/proc_gds_manager"
)

type Dependencies struct {
    syncGdsManager sync_gds_manager.SynchronizeGdsManager
    processGdsManager proc_gds_manager.ProcessGdsManager
}

func NewDependencies() (*Dependencies, error) {
    // @TODO: we must find a way to dynamic configure the whole system without
    //        rebuild code again

    syncGdsManager, err := sync_gds_manager.NewSynchronizeGdsManager(
        PostgresDsnList,
        KafkaNumPartitions, KafkaReeplicationFactor,
        KafkaBrokerList,
        GdsKafkaTopic,
        GdsCommonTimeout)
    if err != nil {
        return nil, err
    }
    
    return &Dependencies{
        syncGdsManager: syncGdsManager,
    }, nil
}
