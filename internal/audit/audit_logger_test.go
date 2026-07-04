package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newAuditLoggerForTest 创建审计日志器并在测试结束时自动关闭，避免 Windows 下
// t.TempDir() 因文件句柄未释放而无法清理。
func newAuditLoggerForTest(t *testing.T, logDir string) *AuditLogger {
	logger := NewAuditLogger(logDir)
	t.Cleanup(func() {
		if err := logger.Close(); err != nil {
			t.Logf("failed to close audit logger: %v", err)
		}
	})
	return logger
}

func TestNewAuditLogger(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	if logger == nil {
		t.Fatal("expected logger, got nil")
	}
}

func TestLog(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	event := &Event{
		TaskID:    "test_task_1",
		EventType: EventTypeSyncStart,
		Schema:    "test_db",
		TableName: "users",
	}
	if err := logger.Log(event); err != nil {
		t.Fatalf("failed to log event: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	files, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("failed to read log directory: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected at least 1 log file, got 0")
	}
	logFile := filepath.Join(logDir, files[0].Name())
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	found := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var loggedEvent Event
		if err := json.Unmarshal([]byte(line), &loggedEvent); err != nil {
			continue
		}
		if loggedEvent.TaskID == event.TaskID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find event with taskID %s", event.TaskID)
	}
}

func TestLogSyncStart(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncStart("task_sync_start", "test_db", "users")

	events, err := logger.Query(QueryOptions{TaskID: "task_sync_start"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeSyncStart {
		t.Errorf("expected event type %s, got %s", EventTypeSyncStart, events[0].EventType)
	}
}

func TestLogSyncComplete(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncComplete("task_sync_complete", "test_db", "users", 1000)
	events, err := logger.Query(QueryOptions{TaskID: "task_sync_complete"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].RowsAffected != 1000 {
		t.Errorf("expected rows affected 1000, got %d", events[0].RowsAffected)
	}
}

func TestLogSyncFailed(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	details := map[string]interface{}{"retry": float64(3)}
	logger.LogSyncFailed("task_sync_failed", "test_db", "users", "binlog.001:123", "connection failed", details)
	events, err := logger.Query(QueryOptions{TaskID: "task_sync_failed"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].Success {
		t.Error("expected success to be false")
	}
	if events[0].ErrorMsg != "connection failed" {
		t.Errorf("expected error message 'connection failed', got '%s'", events[0].ErrorMsg)
	}
}

func TestLogDataWrite(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogDataWrite("task_data_write", "test_db", "users", 100, true, "")
	events, err := logger.Query(QueryOptions{TaskID: "task_data_write"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeDataWrite {
		t.Errorf("expected event type %s, got %s", EventTypeDataWrite, events[0].EventType)
	}
	if events[0].RowsAffected != 100 {
		t.Errorf("expected rows affected 100, got %d", events[0].RowsAffected)
	}
}

func TestLogDataUpdate(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	details := map[string]interface{}{"columns": float64(5)}
	logger.LogDataUpdate("task_data_update", "test_db", "users", true, "", details)
	events, err := logger.Query(QueryOptions{TaskID: "task_data_update"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeDataUpdate {
		t.Errorf("expected event type %s, got %s", EventTypeDataUpdate, events[0].EventType)
	}
}

func TestLogDataDelete(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogDataDelete("task_data_delete", "test_db", "users", true, "")
	events, err := logger.Query(QueryOptions{TaskID: "task_data_delete"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeDataDelete {
		t.Errorf("expected event type %s, got %s", EventTypeDataDelete, events[0].EventType)
	}
}

func TestLogError(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	details := map[string]interface{}{"code": float64(500)}
	logger.LogError("task_error", "internal error", details)
	events, err := logger.Query(QueryOptions{TaskID: "task_error"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeError {
		t.Errorf("expected event type %s, got %s", EventTypeError, events[0].EventType)
	}
	if events[0].Success {
		t.Error("expected success to be false for error event")
	}
}

func TestLogTaskCreated(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogTaskCreated("task_created", "test_task")
	events, err := logger.Query(QueryOptions{TaskID: "task_created"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeTaskCreated {
		t.Errorf("expected event type %s, got %s", EventTypeTaskCreated, events[0].EventType)
	}
}

func TestLogTaskDeleted(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogTaskDeleted("task_deleted")
	events, err := logger.Query(QueryOptions{TaskID: "task_deleted"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeTaskDeleted {
		t.Errorf("expected event type %s, got %s", EventTypeTaskDeleted, events[0].EventType)
	}
}

func TestLogTaskPaused(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogTaskPaused("task_paused")
	events, err := logger.Query(QueryOptions{TaskID: "task_paused"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeTaskPaused {
		t.Errorf("expected event type %s, got %s", EventTypeTaskPaused, events[0].EventType)
	}
}

func TestLogTaskResumed(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogTaskResumed("task_resumed")
	events, err := logger.Query(QueryOptions{TaskID: "task_resumed"})
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events logged")
	}
	if events[0].EventType != EventTypeTaskResumed {
		t.Errorf("expected event type %s, got %s", EventTypeTaskResumed, events[0].EventType)
	}
}

func TestQueryWithFilters(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncStart("task_query_1", "test_db", "users")
	logger.LogSyncStart("task_query_2", "test_db", "orders")
	logger.LogSyncComplete("task_query_1", "test_db", "users", 1000)
	logger.LogSyncComplete("task_query_2", "test_db", "orders", 2000)
	logger.LogSyncFailed("task_query_1", "test_db", "products", "binlog.001:123", "timeout", nil)
	logger.LogSyncFailed("task_query_2", "test_db", "products", "binlog.002:456", "timeout", nil)
	logger.LogDataWrite("task_query_1", "test_db", "users", 100, true, "")
	logger.LogDataWrite("task_query_2", "test_db", "orders", 200, true, "")

	opts := QueryOptions{
		TaskID: "task_query_1",
	}
	events, err := logger.Query(opts)
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) != 4 {
		t.Errorf("expected 4 events for task_query_1, got %d", len(events))
	}

	syncStartCount := 0
	for _, event := range events {
		if event.EventType == EventTypeSyncStart {
			syncStartCount++
		}
	}
	if syncStartCount != 1 {
		t.Errorf("expected 1 sync start event for task_query_1, got %d", syncStartCount)
	}

	opts = QueryOptions{
		StartTime: timePtr(time.Now().Add(-24 * time.Hour)),
		EndTime:   timePtr(time.Now().Add(1 * time.Hour)),
	}
	events, err = logger.Query(opts)
	if err != nil {
		t.Errorf("failed to query events with time range: %v", err)
	}
	if len(events) != 8 {
		t.Errorf("expected 8 events in time range, got %d", len(events))
	}
}

func TestQueryWithEventType(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncStart("task_query_type_unique", "test_db", "users")
	logger.LogSyncComplete("task_query_type_unique", "test_db", "users", 1000)
	logger.LogDataWrite("task_query_type_unique", "test_db", "users", 100, true, "")

	opts := QueryOptions{
		TaskID:    "task_query_type_unique",
		EventType: EventTypeSyncStart,
	}
	events, err := logger.Query(opts)
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 sync start event, got %d", len(events))
	}
}

func TestQueryWithLimit(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncStart("task_query_limit_unique", "test_db", "users")
	logger.LogSyncComplete("task_query_limit_unique", "test_db", "users", 1000)
	logger.LogDataWrite("task_query_limit_unique", "test_db", "users", 100, true, "")

	opts := QueryOptions{
		TaskID: "task_query_limit_unique",
		Limit:  2,
	}
	events, err := logger.Query(opts)
	if err != nil {
		t.Errorf("failed to query events: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events with limit, got %d", len(events))
	}
}

func TestClose(t *testing.T) {
	logDir := t.TempDir()

	logger := NewAuditLogger(logDir)
	if err := logger.Close(); err != nil {
		t.Errorf("failed to close logger: %v", err)
	}
}

func TestRotateFile(t *testing.T) {
	logDir := t.TempDir()

	logger := newAuditLoggerForTest(t, logDir)
	logger.LogSyncStart("task_rotate_unique", "test_db", "users")

	if err := logger.rotateFile(); err != nil {
		t.Errorf("failed to rotate file: %v", err)
	}

	events, err := logger.Query(QueryOptions{TaskID: "task_rotate_unique"})
	if err != nil {
		t.Errorf("failed to query events after rotate: %v", err)
	}
	if len(events) < 1 {
		t.Errorf("expected at least 1 event after rotate, got %d", len(events))
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
