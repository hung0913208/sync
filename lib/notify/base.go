package notify

import (
    "time"
)

type Handler func(msg string) error

type Notifier interface {
    Register(table string) error
    Notify(handler Handler, timeout time.Duration) error
}

