package sink

import (
	"time"
)

// ChangeEvent 标准增量变更事件模型
// 对外部 Sink 暴露统一语义，不暴露 canal/go-mysql 细节。
// 投递语义：At-Least-Once，不承诺端到端 Exactly-Once。
type ChangeEvent struct {
	TaskID      string                   `json:"task_id"`
	SourceSchema string                  `json:"source_schema"`
	SourceTable  string                  `json:"source_table"`
	EventType    string                  `json:"event_type"` // INSERT, UPDATE, DELETE
	EventTime    time.Time               `json:"event_time"`
	BinlogFile   string                  `json:"binlog_file"`
	BinlogPos    uint32                  `json:"binlog_pos"`
	PrimaryKeys  map[string]interface{}  `json:"primary_keys,omitempty"`
	Before       map[string]interface{}  `json:"before,omitempty"`
	After        map[string]interface{}  `json:"after,omitempty"`
	RawRows      []map[string]interface{} `json:"raw_rows,omitempty"`
	TraceID      string                  `json:"trace_id,omitempty"`
}
