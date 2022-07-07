package sync_gds_manager

import (
	"alpaca.vn/libra/devops/sync/lib/kafka"
	"alpaca.vn/libra/devops/sync/lib/notify"

	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Monitor func(id int, msg string)

type SynchronizeGdsManager interface {
	RegisterTable(service string, notifierIdx int, table string) error
	RegisterMonitor(service string, monitor Monitor)
	HandleSyncGdsToKafka(timeout time.Duration)
	HandleSyncGdsToKafkaInBackground(timeou time.Duration)
}

type implSynchronizeGdsManager struct {
	mapOfNotifiers map[string][]notify.Notifier
	producer       kafka.Producer
	monitors       map[string]Monitor
	running        bool
	wg             *sync.WaitGroup
}

func (self *implSynchronizeGdsManager) RegisterTable(
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

func (self *implSynchronizeGdsManager) RegisterMonitor(
	service string,
	monitor Monitor,
) {
	self.monitors[service] = monitor
}

func (self *implSynchronizeGdsManager) HandleSyncGdsToKafka(timeout time.Duration) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	if self.running == false {
		self.HandleSyncGdsToKafkaInBackground(timeout)
	}

	<-sigchan
	self.Close()
}

func (self *implSynchronizeGdsManager) HandleSyncGdsToKafkaInBackground(timeout time.Duration) {
	self.running = true

	for service, notifiers := range self.mapOfNotifiers {
		for id, notifier := range notifiers {
			self.wg.Add(1)

			go func(notifier notify.Notifier, service string, id int) {
				defer self.wg.Done()

				for {
					if self.running == false {
						break
					}

					// @TODO: push error to log center
					notifier.Notify(self.pushGdsToKafka(service, id), timeout)
				}
			}(notifier, service, id)
		}
	}
}

func (self *implSynchronizeGdsManager) Close() {
	self.running = false
	self.wg.Wait()
}

func (self *implSynchronizeGdsManager) pushGdsToKafka(service string, id int) notify.Handler {
	return func(msg string) error {
		err := self.producer.PublishMessage(service, msg)
		if err == nil {
			if monitor, ok := self.monitors[service]; ok {
				monitor(id, msg)
			}
		}
		return err
	}
}

func NewSynchronizeGdsManager(
	postgresDsn []string,
	numPartitions, replicationFactor int,
	brokers []string,
	topic string,
	timeout time.Duration,
) (SynchronizeGdsManager, error) {
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

	return &implSynchronizeGdsManager{
		mapOfNotifiers: mapOfNotifiers,
		producer:       producer,
		monitors:       make(map[string]Monitor),
		wg:             &sync.WaitGroup{},
	}, nil
}
