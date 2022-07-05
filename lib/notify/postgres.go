package notify

import (
  "github.com/lib/pq"    

  "database/sql"
  "errors"
  "time"
  "fmt"
)

type implPostgresNotifier struct {
    listener *pq.Listener
}

func (self *implPostgresNotifier) Notify(handler Handler, timeout time.Duration) error {
  for {
    select {
    case n := <-self.listener.Notify:
      return handler(n.Extra)

    case <-time.After(timeout):
      go func() {
        self.listener.Ping()
      }()

      return errors.New("timeout")
    }
  }
}

func reportIssue(ev pq.ListenerEventType, err error) {
    if err != nil {
        fmt.Println(err.Error())
    }
}

func NewPostgresNotifier(dsn string) (Notifier, error) {
    _, err := sql.Open("postgres", dsn)
    if err != nil {
        return nil, err
    }

    listener := pq.NewListener(dsn, 
                               10 * time.Second, 
                               time.Minute, reportIssue)

    err = listener.Listen("event_channel") 
    if err != nil {
        return nil, err
    }

    return &implPostgresNotifier{listener}, nil
}
