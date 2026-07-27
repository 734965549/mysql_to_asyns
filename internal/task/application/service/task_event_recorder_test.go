package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
	eventStorage "mysql-to-sync/internal/task/infrastructure/storage"
)

func TestTaskEventRecorder_EmitAndAggregate(t *testing.T) {
	oldWindow := eventAggWindow
	eventAggWindow = 200 * time.Millisecond
	t.Cleanup(func() { eventAggWindow = oldWindow })

	dir := t.TempDir()
	store, err := eventStorage.NewFileTaskEventStore(dir)
	require.NoError(t, err)
	rec := NewTaskEventRecorder(store)
	defer rec.Close()
	rec.SetExecutionID("t1", "exec-1")

	rec.Emit(taskEntity.TaskEvent{
		TaskID:     "t1",
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityDiagnostic,
		Category:   taskEntity.EventCategoryTable,
		Code:       "DIAG_PROGRESS",
		Message:    "progress tick",
	})
	rec.Emit(taskEntity.TaskEvent{
		TaskID:     "t1",
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityDiagnostic,
		Category:   taskEntity.EventCategoryTable,
		Code:       "DIAG_PROGRESS",
		Message:    "progress tick",
	})
	rec.Emit(taskEntity.TaskEvent{
		TaskID:     "t1",
		Severity:   taskEntity.EventSeverityError,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryLifecycle,
		Code:       taskEntity.EventCodeTaskFailed,
		Message:    "failed hard",
	})

	time.Sleep(400 * time.Millisecond)
	rec.Close()

	events, err := rec.ListEvents(port.TaskEventListFilter{TaskID: "t1", Limit: 50})
	require.NoError(t, err)
	require.NotEmpty(t, events)

	var hasFailed, hasRepeated bool
	for _, ev := range events {
		if ev.Code == taskEntity.EventCodeTaskFailed {
			hasFailed = true
		}
		if ev.Code == "DIAG_PROGRESS_REPEATED" {
			hasRepeated = true
			assert.GreaterOrEqual(t, ev.RepeatCount, 2)
		}
	}
	assert.True(t, hasFailed)
	assert.True(t, hasRepeated)
}
