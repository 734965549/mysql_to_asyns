package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
	"mysql-to-sync/pkg/logger"
)

// MySQLTaskEventStore MySQL 任务事件存储。
type MySQLTaskEventStore struct {
	db *sql.DB
	mu sync.Mutex
}

// NewMySQLTaskEventStore 创建 MySQL 事件存储并建表。
func NewMySQLTaskEventStore(db *sql.DB) (*MySQLTaskEventStore, error) {
	s := &MySQLTaskEventStore{db: db}
	if err := s.initTable(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *MySQLTaskEventStore) initTable() error {
	query := `
CREATE TABLE IF NOT EXISTS sys_sync_task_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  task_id VARCHAR(64) NOT NULL,
  execution_id VARCHAR(64) NOT NULL,
  seq BIGINT NOT NULL,
  event_id VARCHAR(64) NOT NULL,
  occurred_at TIMESTAMP(3) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  visibility VARCHAR(16) NOT NULL,
  category VARCHAR(32) NOT NULL DEFAULT '',
  code VARCHAR(64) NOT NULL,
  phase VARCHAR(64) NOT NULL DEFAULT '',
  source_schema VARCHAR(128) NOT NULL DEFAULT '',
  source_table VARCHAR(256) NOT NULL DEFAULT '',
  message TEXT NOT NULL,
  details JSON NULL,
  repeat_count INT NOT NULL DEFAULT 0,
  first_at TIMESTAMP(3) NULL,
  last_at TIMESTAMP(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_task_seq (task_id, seq),
  KEY idx_task_execution_seq (task_id, execution_id, seq),
  KEY idx_task_severity_seq (task_id, severity, seq),
  KEY idx_task_code_seq (task_id, code, seq),
  KEY idx_occurred_at (occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := s.db.Exec(query); err != nil {
		return fmt.Errorf("init sys_sync_task_events: %w", err)
	}
	return nil
}

func (s *MySQLTaskEventStore) nextSeq(taskID string) (int64, error) {
	var maxSeq sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(seq) FROM sys_sync_task_events WHERE task_id = ?`, taskID).Scan(&maxSeq)
	if err != nil {
		return 0, err
	}
	if maxSeq.Valid {
		return maxSeq.Int64 + 1, nil
	}
	return 1, nil
}

// Append 追加事件。
func (s *MySQLTaskEventStore) Append(event *taskEntity.TaskEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if err := taskEntity.ValidateTaskEvent(event); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	seq, err := s.nextSeq(event.TaskID)
	if err != nil {
		return err
	}
	event.Seq = seq

	var details []byte
	if len(event.Details) > 0 {
		details, err = json.Marshal(event.Details)
		if err != nil {
			return err
		}
	}
	_, err = s.db.Exec(`
INSERT INTO sys_sync_task_events (
  task_id, execution_id, seq, event_id, occurred_at,
  severity, visibility, category, code, phase,
  source_schema, source_table, message, details,
  repeat_count, first_at, last_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.TaskID, event.ExecutionID, event.Seq, event.EventID, event.Timestamp,
		string(event.Severity), string(event.Visibility), string(event.Category), event.Code, event.Phase,
		event.SourceSchema, event.SourceTable, event.Message, nullJSON(details),
		event.RepeatCount, event.FirstAt, event.LastAt,
	)
	return err
}

func nullJSON(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return b
}

func (s *MySQLTaskEventStore) buildListQuery(filter port.TaskEventListFilter) (string, []interface{}) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	var b strings.Builder
	args := []interface{}{filter.TaskID}
	b.WriteString(`SELECT seq, event_id, task_id, execution_id, occurred_at,
  severity, visibility, category, code, phase, source_schema, source_table,
  message, details, repeat_count, first_at, last_at
FROM sys_sync_task_events WHERE task_id = ?`)
	if filter.ExecutionID != "" {
		b.WriteString(` AND execution_id = ?`)
		args = append(args, filter.ExecutionID)
	}
	if filter.AfterSeq > 0 {
		b.WriteString(` AND seq > ?`)
		args = append(args, filter.AfterSeq)
	}
	if filter.BeforeSeq > 0 {
		b.WriteString(` AND seq < ?`)
		args = append(args, filter.BeforeSeq)
	}
	if filter.MinSeverity != "" {
		switch filter.MinSeverity {
		case taskEntity.EventSeverityWarn:
			b.WriteString(` AND severity IN ('WARN','ERROR')`)
		case taskEntity.EventSeverityError:
			b.WriteString(` AND severity = 'ERROR'`)
		}
	}
	if filter.Visibility != "" {
		b.WriteString(` AND visibility = ?`)
		args = append(args, string(filter.Visibility))
	}
	if filter.Category != "" {
		b.WriteString(` AND category = ?`)
		args = append(args, string(filter.Category))
	}
	if filter.Code != "" {
		b.WriteString(` AND code = ?`)
		args = append(args, filter.Code)
	}
	if filter.SourceSchema != "" {
		b.WriteString(` AND source_schema = ?`)
		args = append(args, filter.SourceSchema)
	}
	if filter.SourceTable != "" {
		b.WriteString(` AND source_table = ?`)
		args = append(args, filter.SourceTable)
	}
	b.WriteString(` ORDER BY seq DESC LIMIT ?`)
	args = append(args, limit)
	return b.String(), args
}

func scanTaskEvent(row interface {
	Scan(dest ...interface{}) error
}) (*taskEntity.TaskEvent, error) {
	var ev taskEntity.TaskEvent
	var sev, vis, cat string
	var details sql.NullString
	var firstAt, lastAt sql.NullTime
	if err := row.Scan(
		&ev.Seq, &ev.EventID, &ev.TaskID, &ev.ExecutionID, &ev.Timestamp,
		&sev, &vis, &cat, &ev.Code, &ev.Phase, &ev.SourceSchema, &ev.SourceTable,
		&ev.Message, &details, &ev.RepeatCount, &firstAt, &lastAt,
	); err != nil {
		return nil, err
	}
	ev.Severity = taskEntity.EventSeverity(sev)
	ev.Visibility = taskEntity.EventVisibility(vis)
	ev.Category = taskEntity.EventCategory(cat)
	if details.Valid && details.String != "" {
		_ = json.Unmarshal([]byte(details.String), &ev.Details)
	}
	if firstAt.Valid {
		t := firstAt.Time
		ev.FirstAt = &t
	}
	if lastAt.Valid {
		t := lastAt.Time
		ev.LastAt = &t
	}
	return &ev, nil
}

// List 列出事件。
func (s *MySQLTaskEventStore) List(filter port.TaskEventListFilter) ([]*taskEntity.TaskEvent, error) {
	if filter.TaskID == "" {
		return nil, fmt.Errorf("task_id is required")
	}
	query, args := s.buildListQuery(filter)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*taskEntity.TaskEvent
	for rows.Next() {
		ev, err := scanTaskEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ListExecutions 列出 execution 摘要。
func (s *MySQLTaskEventStore) ListExecutions(taskID string) ([]*taskEntity.TaskEventExecution, error) {
	rows, err := s.db.Query(`
SELECT execution_id, MIN(occurred_at), MAX(occurred_at), COUNT(*)
FROM sys_sync_task_events WHERE task_id = ?
GROUP BY execution_id ORDER BY MIN(occurred_at) DESC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*taskEntity.TaskEventExecution
	for rows.Next() {
		var ex taskEntity.TaskEventExecution
		var end time.Time
		if err := rows.Scan(&ex.ExecutionID, &ex.StartedAt, &end, &ex.EventCount); err != nil {
			return nil, err
		}
		ex.EndedAt = &end
		out = append(out, &ex)
	}
	return out, rows.Err()
}

// DeleteByTask 删除任务全部事件。
func (s *MySQLTaskEventStore) DeleteByTask(taskID string) error {
	_, err := s.db.Exec(`DELETE FROM sys_sync_task_events WHERE task_id = ?`, taskID)
	return err
}

// Prune 清理过期/过量事件。
func (s *MySQLTaskEventStore) Prune(taskID string, opts port.TaskEventPruneOptions) (int, error) {
	all, err := s.loadAllAsc(taskID)
	if err != nil {
		return 0, err
	}
	if len(all) == 0 {
		return 0, nil
	}
	keep := selectEventsToKeep(all, opts)
	if len(keep) == len(all) {
		return 0, nil
	}
	keepSeq := make(map[int64]struct{}, len(keep))
	for _, ev := range keep {
		keepSeq[ev.Seq] = struct{}{}
	}
	var removeSeqs []int64
	for _, ev := range all {
		if _, ok := keepSeq[ev.Seq]; !ok {
			removeSeqs = append(removeSeqs, ev.Seq)
		}
	}
	if len(removeSeqs) == 0 {
		return 0, nil
	}
	removed := 0
	const batch = 200
	for i := 0; i < len(removeSeqs); i += batch {
		end := i + batch
		if end > len(removeSeqs) {
			end = len(removeSeqs)
		}
		chunk := removeSeqs[i:end]
		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]interface{}, 0, len(chunk)+1)
		args = append(args, taskID)
		for _, seq := range chunk {
			args = append(args, seq)
		}
		res, err := s.db.Exec(
			fmt.Sprintf(`DELETE FROM sys_sync_task_events WHERE task_id = ? AND seq IN (%s)`, placeholders),
			args...,
		)
		if err != nil {
			logger.Warn("task event prune delete failed task=%s: %v", taskID, err)
			continue
		}
		n, _ := res.RowsAffected()
		removed += int(n)
	}
	return removed, nil
}

func (s *MySQLTaskEventStore) loadAllAsc(taskID string) ([]*taskEntity.TaskEvent, error) {
	rows, err := s.db.Query(`
SELECT seq, event_id, task_id, execution_id, occurred_at,
  severity, visibility, category, code, phase, source_schema, source_table,
  message, details, repeat_count, first_at, last_at
FROM sys_sync_task_events WHERE task_id = ? ORDER BY seq ASC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*taskEntity.TaskEvent
	for rows.Next() {
		ev, err := scanTaskEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

var _ port.TaskEventStore = (*MySQLTaskEventStore)(nil)
