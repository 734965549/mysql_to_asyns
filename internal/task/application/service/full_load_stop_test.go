package service

import (
	"context"
	"errors"
	"testing"

	"mysql-to-sync/internal/sync/fullload"
	taskEntity "mysql-to-sync/internal/task/domain/entity"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAbortFullSyncIfCancelled_FailedStatusNotUserStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-failed": {
				Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusFailed},
			},
		},
	}
	err := ts.abortFullSyncIfCancelled(context.Background(), "task-failed")
	assert.NoError(t, err)
	assert.False(t, errors.Is(err, errFullSyncStoppedByUser))
}

func TestAbortFullSyncIfCancelled_PausedStillUserStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-paused": {
				Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused},
			},
		},
	}
	err := ts.abortFullSyncIfCancelled(context.Background(), "task-paused")
	require.ErrorIs(t, err, errFullSyncStoppedByUser)
}

func TestAbortFullSyncIfCancelled_StoppedStillUserStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-stopped": {
				Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusStopped},
			},
		},
	}
	err := ts.abortFullSyncIfCancelled(context.Background(), "task-stopped")
	require.ErrorIs(t, err, errFullSyncStoppedByUser)
}

func TestAbortFullSyncIfCancelled_SchemaLockLostPriority(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-paused": {
				Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused},
			},
		},
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(fullload.ErrSchemaLockLost)
	err := ts.abortFullSyncIfCancelled(ctx, "task-paused")
	require.ErrorIs(t, err, fullload.ErrSchemaLockLost)
}

func TestTaskStopReason_ClassifiesStatuses(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"running": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusRunning}},
			"paused":  {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused}},
			"stopped": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusStopped}},
			"failed":  {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusFailed}},
		},
	}
	assert.Equal(t, taskStopReasonRunning, ts.taskStopReason("running"))
	assert.Equal(t, taskStopReasonPaused, ts.taskStopReason("paused"))
	assert.Equal(t, taskStopReasonStopped, ts.taskStopReason("stopped"))
	assert.Equal(t, taskStopReasonFailed, ts.taskStopReason("failed"))
	assert.Equal(t, taskStopReasonMissing, ts.taskStopReason("missing"))
}

func TestFullLoadStopCause_MapsPauseAndStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"paused":  {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusPaused}},
			"stopped": {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusStopped}},
			"failed":  {Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusFailed}},
		},
	}
	assert.ErrorIs(t, ts.fullLoadStopCause("paused"), fullload.ErrUserPaused)
	assert.ErrorIs(t, ts.fullLoadStopCause("stopped"), fullload.ErrUserStopped)
	assert.NoError(t, ts.fullLoadStopCause("failed"))
}

func TestPairFailureAfterInnerFailedRace_NotUserStop(t *testing.T) {
	ts := &TaskService{
		tasks: map[string]*taskEntity.SyncTask{
			"task-1": {
				Context: taskEntity.ProcessContext{Status: taskEntity.TaskStatusFailed},
			},
		},
	}
	pairErr := errors.New("writer batch failed")
	err := ts.abortFullSyncIfCancelled(context.Background(), "task-1")
	assert.NoError(t, err)
	assert.False(t, errors.Is(err, errFullSyncStoppedByUser))
	_ = pairErr
}
