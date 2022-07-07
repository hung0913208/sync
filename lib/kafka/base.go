package kafka

type Handler func(msg []interface{}) error

type Producer interface {
    PublishMessage(key string, msg interface{}) error
    PublishBatchMultipleKey(keys []string, msg []interface{}) error
    PublishBatchWithSameKey(key string, msg []interface{}) error
    Flush()
    Close()
}

type Consumer interface {
    Subscribe(key string, handler Handler) error
    HandleError(err error)
    Lock()
    Unlock()
    Close()
}

