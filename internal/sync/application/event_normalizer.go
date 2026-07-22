package application

import (
	"fmt"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/sync/domain/sink"
	"mysql-to-sync/pkg/binlog"
)

// ChangeEventTraceID 为单行 binlog 变更生成稳定幂等键，供 Webhook Idempotency-Key 与下游去重。
func ChangeEventTraceID(taskID, binlogFile string, binlogPos uint32, rowIndex int) string {
	return fmt.Sprintf("%s:%s:%d:%d", taskID, binlogFile, binlogPos, rowIndex)
}

func ToChangeEvent(binlogEvent *binlog.BinlogEvent, identity *entity.TableIdentity, taskID string, rowIndex int) (*sink.ChangeEvent, error) {
	if binlogEvent == nil {
		return nil, fmt.Errorf("binlog event is required")
	}
	if identity == nil {
		return nil, fmt.Errorf("table identity is required")
	}
	event := &sink.ChangeEvent{
		TaskID:       taskID,
		SourceSchema: binlogEvent.Schema,
		SourceTable:  binlogEvent.Table,
		EventType:    string(binlogEvent.EventType),
		EventTime:    binlogEvent.Timestamp,
		BinlogFile:   binlogEvent.Position.Name,
		BinlogPos:    binlogEvent.Position.Pos,
		TraceID:      ChangeEventTraceID(taskID, binlogEvent.Position.Name, binlogEvent.Position.Pos, rowIndex),
	}

	primaryKeys := make(map[string]interface{})

	switch binlogEvent.EventType {
	case binlog.EventTypeInsert:
		if len(binlogEvent.Rows) == 0 {
			return nil, fmt.Errorf("INSERT event has no rows")
		}
		event.After = binlogEvent.Rows[0]
		for _, col := range identity.IdentifyCols {
			if v, ok := event.After[col]; ok {
				primaryKeys[col] = v
			}
		}

	case binlog.EventTypeUpdate:
		if len(binlogEvent.Rows) == 0 {
			return nil, fmt.Errorf("UPDATE event has no rows")
		}
		event.After = binlogEvent.Rows[0]
		if len(binlogEvent.BeforeImage) > 0 {
			event.Before = binlogEvent.BeforeImage[0]
		}
		for _, col := range identity.IdentifyCols {
			if v, ok := event.After[col]; ok {
				primaryKeys[col] = v
			}
		}

	case binlog.EventTypeDelete:
		if len(binlogEvent.BeforeImage) > 0 {
			event.Before = binlogEvent.BeforeImage[0]
			for _, col := range identity.IdentifyCols {
				if v, ok := event.Before[col]; ok {
					primaryKeys[col] = v
				}
			}
		} else if len(binlogEvent.Rows) > 0 {
			event.Before = binlogEvent.Rows[0]
			for _, col := range identity.IdentifyCols {
				if v, ok := event.Before[col]; ok {
					primaryKeys[col] = v
				}
			}
		} else {
			return nil, fmt.Errorf("DELETE event has no rows")
		}

	default:
		return nil, fmt.Errorf("unknown binlog event type: %s", binlogEvent.EventType)
	}

	event.PrimaryKeys = primaryKeys
	return event, nil
}
