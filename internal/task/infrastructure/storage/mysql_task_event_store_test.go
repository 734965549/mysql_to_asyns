package storage

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

func TestMySQLTaskEventStore_AppendAndList(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS sys_sync_task_events").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT MAX(seq) FROM sys_sync_task_events WHERE task_id = ?")).
		WithArgs("t1").
		WillReturnRows(sqlmock.NewRows([]string{"MAX(seq)"}).AddRow(nil))
	mock.ExpectExec("INSERT INTO sys_sync_task_events").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT seq, event_id").
		WillReturnRows(sqlmock.NewRows([]string{
			"seq", "event_id", "task_id", "execution_id", "occurred_at",
			"severity", "visibility", "category", "code", "phase",
			"source_schema", "source_table", "message", "details",
			"repeat_count", "first_at", "last_at",
		}).AddRow(
			1, "e1", "t1", "exec", time.Now(),
			"INFO", "KEY", "LIFECYCLE", "TASK_STARTED", "",
			"", "", "started", nil,
			0, nil, nil,
		))

	store, err := NewMySQLTaskEventStore(db)
	require.NoError(t, err)
	require.NoError(t, store.Append(&taskEntity.TaskEvent{
		EventID:     "e1",
		TaskID:      "t1",
		ExecutionID: "exec",
		Timestamp:   time.Now(),
		Severity:    taskEntity.EventSeverityInfo,
		Visibility:  taskEntity.EventVisibilityKey,
		Category:    taskEntity.EventCategoryLifecycle,
		Code:        taskEntity.EventCodeTaskStarted,
		Message:     "started",
	}))
	events, err := store.List(port.TaskEventListFilter{TaskID: "t1", Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}
