package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

func runTaskEventStoreContract(t *testing.T, name string, factory func(t *testing.T) port.TaskEventStore) {
	t.Run(name, func(t *testing.T) {
		store := factory(t)
		taskID := "contract-" + name
		exec := "exec-1"
		for i := 0; i < 3; i++ {
			require.NoError(t, store.Append(sampleEvent(taskID, exec, taskEntity.EventCodeTaskStarted)))
		}

		all, err := store.List(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
		require.NoError(t, err)
		require.Len(t, all, 3)
		require.Equal(t, int64(3), all[0].Seq)

		page, err := store.List(port.TaskEventListFilter{TaskID: taskID, AfterSeq: 1, Limit: 10})
		require.NoError(t, err)
		require.Len(t, page, 2)

		execs, err := store.ListExecutions(taskID)
		require.NoError(t, err)
		require.Len(t, execs, 1)

		require.NoError(t, store.DeleteByTask(taskID))
		left, err := store.List(port.TaskEventListFilter{TaskID: taskID})
		require.NoError(t, err)
		require.Empty(t, left)
	})
}

func TestTaskEventStoreContract_File(t *testing.T) {
	runTaskEventStoreContract(t, "file", func(t *testing.T) port.TaskEventStore {
		dir := t.TempDir()
		store, err := NewFileTaskEventStore(dir)
		require.NoError(t, err)
		return store
	})
}

func TestTaskEventStoreContract_FileSeqRecoveryMatchesMySQLSemantics(t *testing.T) {
	dir := t.TempDir()
	store1, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)
	taskID := "parity-task"
	require.NoError(t, store1.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskStarted)))

	store2, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)
	require.NoError(t, store2.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskFailed)))
	events, err := store2.List(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, int64(2), events[0].Seq)
	require.Equal(t, taskEntity.EventCodeTaskFailed, events[0].Code)
}

func TestTaskEventStoreContract_PruneKeepsErrorEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileTaskEventStore(dir)
	require.NoError(t, err)
	taskID := "prune-task"
	for i := 0; i < 5; i++ {
		require.NoError(t, store.Append(sampleEvent(taskID, "exec", taskEntity.EventCodeTaskStarted)))
	}
	require.NoError(t, store.Append(&taskEntity.TaskEvent{
		EventID: "err", TaskID: taskID, ExecutionID: "exec", Timestamp: time.Now(),
		Severity: taskEntity.EventSeverityError, Visibility: taskEntity.EventVisibilityKey,
		Category: taskEntity.EventCategoryLifecycle, Code: taskEntity.EventCodeTaskFailed, Message: "fail",
	}))

	removed, err := store.Prune(taskID, port.TaskEventPruneOptions{
		MaxKeyEvents: 2,
		MinErrorEvents: 1,
		MaxAge:        30 * 24 * time.Hour,
	})
	require.NoError(t, err)
	require.Greater(t, removed, 0)

	left, err := store.List(port.TaskEventListFilter{TaskID: taskID, Limit: 50})
	require.NoError(t, err)
	var hasError bool
	for _, ev := range left {
		if ev.Code == taskEntity.EventCodeTaskFailed {
			hasError = true
		}
	}
	require.True(t, hasError)
}
