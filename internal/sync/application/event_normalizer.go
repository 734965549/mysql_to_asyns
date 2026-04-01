package application

import (
	"fmt"
	"mysql-to-async/internal/sync/domain/sink"
	"mysql-to-async/pkg/binlog"
)

// EventNormalizer 负责把 pkg/binlog.BinlogEvent 转成标准 ChangeEvent
type EventNormalizer struct{}

// NewEventNormalizer 创建事件标准化器
func NewEventNormalizer() *EventNormalizer {
	return &EventNormalizer{}
}

// Normalize 将原始 BinlogEvent 转换为标准 ChangeEvent 列表
// 对于 INSERT/DELETE 事件，每行产生一个 ChangeEvent
// 对于 UPDATE 事件，Rows 与 BeforeImage 一一对应，每行产生一个 ChangeEvent（同时携带 Before 与 After）
func (n *EventNormalizer) Normalize(taskID string, event *binlog.BinlogEvent) ([]*sink.ChangeEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}

	var events []*sink.ChangeEvent

	switch event.EventType {
	case binlog.EventTypeInsert:
		for _, row := range event.Rows {
			ce := &sink.ChangeEvent{
				TaskID:       taskID,
				SourceSchema: event.Schema,
				SourceTable:  event.Table,
				EventType:    string(event.EventType),
				EventTime:    event.Timestamp,
				BinlogFile:   event.Position.Name,
				BinlogPos:    event.Position.Pos,
				After:        row,
				RawRows:      event.Rows,
			}
			events = append(events, ce)
		}

	case binlog.EventTypeUpdate:
		for i, row := range event.Rows {
			ce := &sink.ChangeEvent{
				TaskID:       taskID,
				SourceSchema: event.Schema,
				SourceTable:  event.Table,
				EventType:    string(event.EventType),
				EventTime:    event.Timestamp,
				BinlogFile:   event.Position.Name,
				BinlogPos:    event.Position.Pos,
				After:        row,
				RawRows:      event.Rows,
			}
			if i < len(event.BeforeImage) {
				ce.Before = event.BeforeImage[i]
			}
			events = append(events, ce)
		}

	case binlog.EventTypeDelete:
		for _, row := range event.BeforeImage {
			ce := &sink.ChangeEvent{
				TaskID:       taskID,
				SourceSchema: event.Schema,
				SourceTable:  event.Table,
				EventType:    string(event.EventType),
				EventTime:    event.Timestamp,
				BinlogFile:   event.Position.Name,
				BinlogPos:    event.Position.Pos,
				Before:       row,
				RawRows:      event.Rows,
			}
			events = append(events, ce)
		}

	default:
		return nil, fmt.Errorf("unknown event type: %s", event.EventType)
	}

	return events, nil
}
