package notify

import (
    "github.com/lib/pq"    

    "gorm.io/driver/postgres"
    "gorm.io/gorm"

    "database/sql"
    "errors"
    "time"
    "fmt"
)

type implPostgresNotifier struct {
    listener *pq.Listener
    dbconn   *gorm.DB
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

func (self *implPostgresNotifier) Register(table string) error {
    query := fmt.Sprintf("create trigger %s_trigger after insert or update on %s " + 
                         "for each row execute procedure notify_message();",
                         table, table)
    tx := self.dbconn.Exec(query)
    return tx.Error
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

    dbconn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
    if err != nil {
        return nil, err
    }
 
    return &implPostgresNotifier{listener, dbconn}, nil
}
