package db

type GdsMessage struct {
    Action string                   `json:"action"`
    Schema string                   `json:"schema"`
    Table  string                   `json:"table"`
    New    map[string]interface{}   `json:"new"`
    Old    map[string]interface{}   `json:"old"`
}
