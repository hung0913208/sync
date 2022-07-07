package sync_gds_manager

import (
    "alpaca.vn/libra/devops/sync/lib/kafka"
    "alpaca.vn/libra/devops/sync/lib/notify"

    "os/signal"
    "syscall"
    "sync"
    "time"
    "fmt"
    "os"
)

type Monitor func(id int, msg string)

type SyncGdsManager interface {
    RegisterTable(service string, notifierIdx int, table string) error
    RegisterMonitor(service string, monitor Monitor)
    HandleSyncGdsToKafka(timeout time.Duration)
}

type implSyncGdsManager struct {
    mapOfNotifiers  map[string][]notify.Notifier
    producer        kafka.Producer
    monitors        map[string]Monitor
}

func (self *implSyncGdsManager) RegisterTable(
    service string,
    notifierIdx int,
    table string,
) error {
    if _, ok := self.mapOfNotifiers[service]; !ok {
        return fmt.Errorf("Don't support service %s", service)
    }

    if notifierIdx >= len(self.mapOfNotifiers[service]) {
        return fmt.Errorf("notifierIdx is out of range")
    }

    return self.mapOfNotifiers[service][notifierIdx].Register(table)
}

func (self *implSyncGdsManager) RegisterMonitor(
    service string, 
    monitor Monitor,
) {
    self.monitors[service] = monitor
}

func (self *implSyncGdsManager) HandleSyncGdsToKafka(timeout time.Duration) {
    wg := &sync.WaitGroup{}
    running := true
    sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    for service, notifiers := range self.mapOfNotifiers {
        for id, notifier := range notifiers {
            wg.Add(1)

            go func(notifier notify.Notifier) {
                defer wg.Done()

                for {
                    if running == false {
                        break
                    }

                    // @TODO: push error to log center
                    notifier.Notify(self.pushGdsToKafka(service, id), timeout)
                }
            } (notifier)
        }
    }

    <-sigchan
    running = false
    wg.Wait()
}

func (self *implSyncGdsManager) pushGdsToKafka(service string, id int) notify.Handler {
    return func(msg string) error {
        err := self.producer.PublishMessage(service, msg)
        if err == nil {
            fmt.Println("catch", msg)
            if monitor, ok := self.monitors[service]; ok {
                monitor(id, msg)
            } else {
                fmt.Println("not found", msg)
            }
        }
        return err
    }
}

func NewSyncGdsManager(
    postgresDsn []string,
    numPartitions, replicationFactor int,
    brokers []string, 
    topic string,
    timeout time.Duration,
) (SyncGdsManager, error) { 
    postgresNotifiers := make([]notify.Notifier, 0)

    for _, dsn := range postgresDsn {
        notifier, err := notify.NewPostgresNotifier(dsn)
        if err != nil {
            return nil, err
        }

        postgresNotifiers = append(postgresNotifiers, notifier)
    }

    mapOfNotifiers := make(map[string][]notify.Notifier)
    mapOfNotifiers[PostgresNotifierKey] = postgresNotifiers

    producer, err := kafka.NewProducer(
        numPartitions, replicationFactor,
        brokers,
        topic,
        timeout)
    if err != nil {
        return nil, err
    }

    return &implSyncGdsManager{
        mapOfNotifiers: mapOfNotifiers,
        producer:       producer,
        monitors:       make(map[string]Monitor),
    }, nil
}
