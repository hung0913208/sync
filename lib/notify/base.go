package notify

import (
    "time"
)

type Handler func(msg string) error

type Notifier interface {
    Notify(handler Handler, timeout time.Duration) error
}


