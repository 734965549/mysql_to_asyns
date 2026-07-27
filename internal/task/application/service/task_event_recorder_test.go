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

func TestTaskEventRecorder_WarnEventsAggregate(t *testing.T) {
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
		Severity:   taskEntity.EventSeverityWarn,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryRetry,
		Code:       taskEntity.EventCodeWriteLockRetry,
		Message:    "write lock retry",
	})

	time.Sleep(50 * time.Millisecond)
	firstBatch, err := rec.ListEvents(port.TaskEventListFilter{TaskID: "t1", Limit: 50})
	require.NoError(t, err)
	require.NotEmpty(t, firstBatch, "first WARN should persist immediately")
	assert.Equal(t, taskEntity.EventCodeWriteLockRetry, firstBatch[0].Code)

	for i := 0; i < 2; i++ {
		rec.Emit(taskEntity.TaskEvent{
			TaskID:     "t1",
			Severity:   taskEntity.EventSeverityWarn,
			Visibility: taskEntity.EventVisibilityKey,
			Category:   taskEntity.EventCategoryRetry,
			Code:       taskEntity.EventCodeWriteLockRetry,
			Message:    "write lock retry",
		})
	}

	time.Sleep(400 * time.Millisecond)
	rec.Close()

	events, err := rec.ListEvents(port.TaskEventListFilter{TaskID: "t1", Limit: 50})
	require.NoError(t, err)
	var repeated bool
	for _, ev := range events {
		if ev.Code == taskEntity.EventCodeWriteLockRetry+"_REPEATED" {
			repeated = true
			assert.Equal(t, 2, ev.RepeatCount, "summary should count repeats after the first persisted WARN")
		}
	}
	assert.True(t, repeated, "WARN events should aggregate into *_REPEATED")
}

func TestTaskEventRecorder_CloseFlushesPendingAggregates(t *testing.T) {
	oldWindow := eventAggWindow
	eventAggWindow = time.Minute
	t.Cleanup(func() { eventAggWindow = oldWindow })

	dir := t.TempDir()
	store, err := eventStorage.NewFileTaskEventStore(dir)
	require.NoError(t, err)
	rec := NewTaskEventRecorder(store)
	rec.SetExecutionID("t1", "exec-1")
	rec.Emit(taskEntity.TaskEvent{
		TaskID:     "t1",
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityDiagnostic,
		Category:   taskEntity.EventCategoryTable,
		Code:       "DIAG_TICK",
		Message:    "tick",
	})
	rec.Close()

	events, err := rec.ListEvents(port.TaskEventListFilter{TaskID: "t1", Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "DIAG_TICK", events[0].Code)
}

func TestTaskEventRecorder_DeleteByTaskIgnoresQueuedEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := eventStorage.NewFileTaskEventStore(dir)
	require.NoError(t, err)
	rec := NewTaskEventRecorder(store)
	rec.SetExecutionID("t-del", "exec-1")
	rec.queue <- pendingEmit{event: &taskEntity.TaskEvent{
		TaskID:      "t-del",
		ExecutionID: "exec-1",
		EventID:     "queued",
		Timestamp:   time.Now(),
		Severity:    taskEntity.EventSeverityInfo,
		Visibility:  taskEntity.EventVisibilityKey,
		Category:    taskEntity.EventCategoryLifecycle,
		Code:        taskEntity.EventCodeTaskStarted,
		Message:     "queued after delete",
	}, gen: 0}
	require.NoError(t, rec.DeleteByTask("t-del"))
	rec.Close()

	events, err := rec.ListEvents(port.TaskEventListFilter{TaskID: "t-del", Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, events)
}
