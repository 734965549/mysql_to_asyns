package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

func TestFileTaskEventStore_AppendListDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)

	taskID := "task-a"
	for i := 1; i <= 3; i++ {
		require.NoError(t, store.Append(&taskEntity.TaskEvent{
			EventID:     "e",
			TaskID:      taskID,
			ExecutionID: "exec-1",
			Timestamp:   time.Now(),
			Severity:    taskEntity.EventSeverityInfo,
			Visibility:  taskEntity.EventVisibilityKey,
			Category:    taskEntity.EventCategoryLifecycle,
			Code:        taskEntity.EventCodeTaskStarted,
			Message:     "started",
		}))
	}

	events, err := store.List(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, int64(3), events[0].Seq)

	after, err := store.List(port.TaskEventListFilter{TaskID: taskID, AfterSeq: 1, Limit: 10})
	require.NoError(t, err)
	require.Len(t, after, 2)

	execs, err := store.ListExecutions(taskID)
	require.NoError(t, err)
	require.Len(t, execs, 1)

	require.NoError(t, store.DeleteByTask(taskID))
	events, err = store.List(port.TaskEventListFilter{TaskID: taskID})
	require.NoError(t, err)
	require.Empty(t, events)

	path := filepath.Join(dir, safeTaskEventBasename(taskID)+".jsonl")
	require.NoFileExists(t, path)
}

func TestFileTaskEventStore_SeqRecoveryAfterRestart(t *testing.T) {
	dir := t.TempDir()
	store1, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)
	taskID := "task-b"
	require.NoError(t, store1.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskStarted)))
	require.NoError(t, store1.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskCompleted)))

	store2, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)
	require.NoError(t, store2.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskFailed)))
	events, err := store2.List(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, int64(3), events[0].Seq)
}

func sampleEvent(taskID, exec, code string) *taskEntity.TaskEvent {
	return &taskEntity.TaskEvent{
		EventID:     "ev",
		TaskID:      taskID,
		ExecutionID: exec,
		Timestamp:   time.Now(),
		Severity:    taskEntity.EventSeverityInfo,
		Visibility:  taskEntity.EventVisibilityKey,
		Category:    taskEntity.EventCategoryLifecycle,
		Code:        code,
		Message:     code,
	}
}
