package manager

import (
    "alpaca.vn/libra/devops/sync/lib/manager/sync_gds_manager"
)

type Dependencies struct {
    syncGdsManager sync_gds_manager.SyncGdsManager
}

func NewDependencies() (*Dependencies, error) {
    // @TODO: we must find a way to dynamic configure the whole system without
    //        rebuild code again

    syncGdsManager, err := sync_gds_manager.NewSyncGdsManager(
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
