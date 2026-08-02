package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"mysql-to-sync/internal/config"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

func newTaskServiceWithEvents(t *testing.T) *TaskService {
	t.Helper()
	ts := NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: t.TempDir()},
	})
	t.Cleanup(func() { _ = ts.Close() })
	require.NotNil(t, ts.EventRecorder(), "task event infrastructure should be initialized")
	return ts
}

func flushTaskEvents(t *testing.T, ts *TaskService) {
	t.Helper()
	require.NotNil(t, ts.EventRecorder())
	ts.EventRecorder().Close()
}

func listTaskEventCodes(t *testing.T, ts *TaskService, taskID string) []string {
	t.Helper()
	events, err := ts.ListTaskEvents(port.TaskEventListFilter{TaskID: taskID, Limit: 200})
	require.NoError(t, err)
	codes := make([]string, 0, len(events))
	for _, ev := range events {
		codes = append(codes, ev.Code)
	}
	return codes
}

func hasEventCode(events []*taskEntity.TaskEvent, code string) bool {
	for _, ev := range events {
		if ev.Code == code {
			return true
		}
	}
	return false
}

func TestTaskEventLifecycle_StartPauseResume(t *testing.T) {
	ts := newTaskServiceWithEvents(t)
	taskID := "lifecycle_pause_resume"
	_, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:   taskID,
		Name: "Lifecycle",
		Mode: taskEntity.SyncModeFull,
	})
	require.NoError(t, err)

	ts.initRuntimeFn = func(*taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(context.Context, string, *taskRuntime) {}

	require.NoError(t, ts.StartTask(context.Background(), taskID))
	exec1 := ts.EventRecorder().CurrentExecutionID(taskID)
	require.NotEmpty(t, exec1)

	require.NoError(t, ts.PauseTask(taskID))

	require.NoError(t, ts.StartTask(context.Background(), taskID))
	exec2 := ts.EventRecorder().CurrentExecutionID(taskID)
	require.NotEmpty(t, exec2)
	assert.NotEqual(t, exec1, exec2, "each StartTask should bind a new execution_id")

	flushTaskEvents(t, ts)
	codes := listTaskEventCodes(t, ts, taskID)
	assert.Contains(t, codes, taskEntity.EventCodeTaskStarted)
	assert.Contains(t, codes, taskEntity.EventCodeTaskConfigEffective)
	assert.Contains(t, codes, taskEntity.EventCodeTaskPaused)
	assert.Contains(t, codes, taskEntity.EventCodeTaskResumed)

	events, err := ts.ListTaskEvents(port.TaskEventListFilter{TaskID: taskID, Limit: 200})
	require.NoError(t, err)
	var resumedExec string
	for _, ev := range events {
		if ev.Code == taskEntity.EventCodeTaskResumed {
			resumedExec = ev.ExecutionID
			break
		}
	}
	assert.Equal(t, exec2, resumedExec)
}

func TestTaskEventLifecycle_CompleteAndFail(t *testing.T) {
	ts := newTaskServiceWithEvents(t)

	completeID := "lifecycle_complete"
	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: completeID, Name: "Complete"})
	require.NoError(t, err)
	taskComplete, ok := ts.GetTask(completeID)
	require.True(t, ok)
	taskComplete.Start()
	ts.bindExecution(completeID, nil)
	ts.completeTask(completeID)

	failID := "lifecycle_fail"
	_, err = ts.CreateTask(taskEntity.TaskConfig{ID: failID, Name: "Fail"})
	require.NoError(t, err)
	taskFail, ok := ts.GetTask(failID)
	require.True(t, ok)
	taskFail.Start()
	ts.bindExecution(failID, nil)
	ts.failTaskUnlessCancelled(context.Background(), failID, "sync exploded")

	flushTaskEvents(t, ts)

	completeCodes := listTaskEventCodes(t, ts, completeID)
	assert.Contains(t, completeCodes, taskEntity.EventCodeTaskCompleted)

	failCodes := listTaskEventCodes(t, ts, failID)
	assert.Contains(t, failCodes, taskEntity.EventCodeTaskFailed)
}

func TestDeleteTask_ClearsTaskEvents(t *testing.T) {
	ts := newTaskServiceWithEvents(t)
	taskID := "lifecycle_delete_events"
	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Delete events"})
	require.NoError(t, err)

	rec := ts.EventRecorder()
	rec.SetExecutionID(taskID, "exec-delete")
	rec.Emit(taskEntity.TaskEvent{
		TaskID:     taskID,
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryLifecycle,
		Code:       taskEntity.EventCodeTaskStarted,
		Message:    "started for delete test",
	})
	flushTaskEvents(t, ts)

	before, err := ts.ListTaskEvents(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, before)

	require.NoError(t, ts.DeleteTask(taskID))

	after, err := ts.ListTaskEvents(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, after)

	_, exists := ts.GetTask(taskID)
	assert.False(t, exists)
}

func TestListTaskEventExecutions_AfterMultipleStarts(t *testing.T) {
	ts := newTaskServiceWithEvents(t)
	taskID := "lifecycle_executions"
	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Executions", Mode: taskEntity.SyncModeFull})
	require.NoError(t, err)

	ts.initRuntimeFn = func(*taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(context.Context, string, *taskRuntime) {}

	require.NoError(t, ts.StartTask(context.Background(), taskID))
	require.NoError(t, ts.PauseTask(taskID))
	require.NoError(t, ts.StartTask(context.Background(), taskID))
	require.NoError(t, ts.PauseTask(taskID))

	flushTaskEvents(t, ts)

	execs, err := ts.ListTaskEventExecutions(taskID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(execs), 2, "two StartTask rounds should produce at least two executions")
}

func TestTaskEventStorePath_UnderDataDir(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	ts := NewTaskService(&config.Config{
		Storage: config.StorageConfig{Mode: "file", DataDir: dataDir},
	})
	defer ts.Close()

	taskID := "store_path_check"
	_, err := ts.CreateTask(taskEntity.TaskConfig{ID: taskID, Name: "Path"})
	require.NoError(t, err)

	ts.initRuntimeFn = func(*taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(context.Context, string, *taskRuntime) {}
	require.NoError(t, ts.StartTask(context.Background(), taskID))
	require.NoError(t, ts.PauseTask(taskID))

	flushTaskEvents(t, ts)
	events, err := ts.ListTaskEvents(port.TaskEventListFilter{TaskID: taskID, Limit: 10})
	require.NoError(t, err)
	require.True(t, hasEventCode(events, taskEntity.EventCodeTaskStarted))
}

func TestTaskEventLifecycle_FailedRestartClearsContextError(t *testing.T) {
	ts := newTaskServiceWithEvents(t)
	taskID := "lifecycle_failed_restart"
	_, err := ts.CreateTask(taskEntity.TaskConfig{
		ID:   taskID,
		Name: "Failed Restart",
		Mode: taskEntity.SyncModeFull,
	})
	require.NoError(t, err)

	ts.initRuntimeFn = func(*taskEntity.SyncTask) (*taskRuntime, error) {
		return &taskRuntime{}, nil
	}
	ts.executeSyncFn = func(context.Context, string, *taskRuntime) {}

	require.NoError(t, ts.StartTask(context.Background(), taskID))
	exec1 := ts.EventRecorder().CurrentExecutionID(taskID)
	require.NotEmpty(t, exec1)

	task, ok := ts.GetTask(taskID)
	require.True(t, ok)
	ts.failTaskUnlessCancelled(context.Background(), taskID, "round 1 error")
	task, ok = ts.GetTask(taskID)
	require.True(t, ok)
	assert.Equal(t, taskEntity.TaskStatusFailed, task.Context.Status)
	assert.Equal(t, "round 1 error", task.Context.ErrorStack)

	require.NoError(t, ts.StartTask(context.Background(), taskID))
	exec2 := ts.EventRecorder().CurrentExecutionID(taskID)
	require.NotEmpty(t, exec2)
	assert.NotEqual(t, exec1, exec2)

	task, ok = ts.GetTask(taskID)
	require.True(t, ok)
	assert.Equal(t, taskEntity.TaskStatusRunning, task.Context.Status)
	assert.Empty(t, task.Context.ErrorStack)
	assert.True(t, task.Context.EndTime.IsZero())

	flushTaskEvents(t, ts)
	round2Events, err := ts.ListTaskEvents(port.TaskEventListFilter{
		TaskID:      taskID,
		ExecutionID: exec2,
		Limit:       200,
	})
	require.NoError(t, err)
	for _, ev := range round2Events {
		assert.Equal(t, exec2, ev.ExecutionID)
		assert.NotEqual(t, "round 1 error", ev.Message)
	}

	ts.failTaskUnlessCancelled(context.Background(), taskID, "round 2 error")
	task, ok = ts.GetTask(taskID)
	require.True(t, ok)
	assert.Equal(t, "round 2 error", task.Context.ErrorStack)
	assert.NotContains(t, task.Context.ErrorStack, "round 1 error")
}
