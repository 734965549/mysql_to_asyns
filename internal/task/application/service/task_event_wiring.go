package service

import (
	"fmt"
	"path/filepath"

	"github.com/google/uuid"

	"mysql-to-sync/internal/config"
	"mysql-to-sync/internal/sync/fullload"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
	eventStorage "mysql-to-sync/internal/task/infrastructure/storage"
	"mysql-to-sync/pkg/logger"
)

func initTaskEventInfrastructure(ts *TaskService, cfg *config.Config) {
	if ts == nil {
		return
	}
	store, closer, storeType, err := newTaskEventStore(ts, cfg)
	if err != nil {
		logger.Warn("Warning: task events disabled: %v", err)
		return
	}
	ts.eventStoreCloser = closer
	ts.eventRecorder = NewTaskEventRecorder(store)
	logger.Info("Using %s task event storage", storeType)

	pruneCfg := taskEventPruneConfigFrom(cfg)
	ts.pruneStop = make(chan struct{})
	ts.StartTaskEventPruneLoop(pruneCfg)
}

func newTaskEventStore(ts *TaskService, cfg *config.Config) (port.TaskEventStore, func() error, string, error) {
	if mysqlStorage, ok := ts.storage.(*MySQLTaskStorage); ok {
		store, err := eventStorage.NewMySQLTaskEventStore(mysqlStorage.db)
		if err != nil {
			return nil, nil, "", err
		}
		return store, nil, "mysql", nil
	}
	dataDir := "data"
	if cfg != nil && cfg.Storage.DataDir != "" {
		dataDir = cfg.Storage.DataDir
	}
	dir := filepath.Join(dataDir, "task-events")
	store, err := eventStorage.NewFileTaskEventStore(dir)
	if err != nil {
		return nil, nil, "", err
	}
	return store, nil, "file", nil
}

// EventRecorder 返回任务事件 recorder（可能为 nil）。
func (s *TaskService) EventRecorder() *TaskEventRecorder {
	if s == nil {
		return nil
	}
	return s.eventRecorder
}

// ListTaskEvents 列出任务事件。
func (s *TaskService) ListTaskEvents(filter port.TaskEventListFilter) ([]*taskEntity.TaskEvent, error) {
	if s == nil || s.eventRecorder == nil {
		return []*taskEntity.TaskEvent{}, nil
	}
	return s.eventRecorder.ListEvents(filter)
}

// ListTaskEventExecutions 列出 execution 摘要。
func (s *TaskService) ListTaskEventExecutions(taskID string) ([]*taskEntity.TaskEventExecution, error) {
	if s == nil || s.eventRecorder == nil {
		return []*taskEntity.TaskEventExecution{}, nil
	}
	return s.eventRecorder.ListExecutions(taskID)
}

func (s *TaskService) newExecutionID() string {
	return uuid.NewString()
}

func (s *TaskService) bindExecution(taskID string, runtime *taskRuntime) string {
	execID := s.newExecutionID()
	if runtime != nil {
		runtime.executionID = execID
	}
	if s.eventRecorder != nil {
		s.eventRecorder.SetExecutionID(taskID, execID)
	}
	return execID
}

// FullLoadEventSink 适配 fullload.EventSink。
type FullLoadEventSink struct {
	TaskID   string
	Recorder *TaskEventRecorder
}

func (a *FullLoadEventSink) Emit(event fullload.FullLoadEvent) {
	if a == nil || a.Recorder == nil {
		return
	}
	a.Recorder.Emit(taskEntity.TaskEvent{
		TaskID:       a.TaskID,
		Severity:     taskEntity.EventSeverity(event.Severity),
		Visibility:   taskEntity.EventVisibility(event.Visibility),
		Category:     taskEntity.EventCategory(event.Category),
		Code:         event.Code,
		Phase:        event.Phase,
		SourceSchema: event.SourceSchema,
		SourceTable:  event.SourceTable,
		Message:      event.Message,
		Details:      event.Details,
	})
}

func (s *TaskService) emitTaskConfigEffective(task *taskEntity.SyncTask) {
	if s == nil || s.eventRecorder == nil || task == nil {
		return
	}
	s.eventRecorder.Emit(taskEntity.TaskEvent{
		TaskID:     task.Config.ID,
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryConfig,
		Code:       taskEntity.EventCodeTaskConfigEffective,
		Message:    fmt.Sprintf("mode=%s engine=%s workers=%d batch=%d",
			task.Config.Mode, effectiveFullLoadEngineLabel(task.Config),
			task.Config.WorkerCount, task.Config.BatchSize),
		Details: map[string]interface{}{
			"mode":                    task.Config.Mode,
			"full_load_engine":        effectiveFullLoadEngineLabel(task.Config),
			"worker_count":            task.Config.WorkerCount,
			"batch_size":              task.Config.BatchSize,
			"full_load_read_workers":       task.Config.FullLoadReadWorkers,
			"full_load_table_workers":      task.Config.FullLoadTableWorkers,
			"full_load_per_table_readers":  task.Config.FullLoadPerTableReaders,
			"full_load_write_workers":      task.Config.FullLoadWriteWorkers,
		},
	})
}

func (s *TaskService) emitLifecycle(taskID, code, message string, severity taskEntity.EventSeverity) {
	if s == nil || s.eventRecorder == nil {
		return
	}
	s.eventRecorder.EmitLifecycle(taskID, code, message, severity)
}

func (s *TaskService) emitPhase(taskID, code, phase, message string) {
	if s == nil || s.eventRecorder == nil {
		return
	}
	s.eventRecorder.EmitPhase(taskID, code, phase, message)
}

func (s *TaskService) emitTableEvent(taskID, code, schema, table, message string, severity taskEntity.EventSeverity, details map[string]interface{}) {
	if s == nil || s.eventRecorder == nil {
		return
	}
	s.eventRecorder.Emit(taskEntity.TaskEvent{
		TaskID:       taskID,
		Severity:     severity,
		Visibility:   taskEntity.EventVisibilityKey,
		Category:     taskEntity.EventCategoryTable,
		Code:         code,
		SourceSchema: schema,
		SourceTable:  table,
		Message:      message,
		Details:      details,
	})
}

func (s *TaskService) emitRetryEvent(taskID, code, schema, table, message string, severity taskEntity.EventSeverity, details map[string]interface{}) {
	if s == nil || s.eventRecorder == nil {
		return
	}
	s.eventRecorder.Emit(taskEntity.TaskEvent{
		TaskID:       taskID,
		Severity:     severity,
		Visibility:   taskEntity.EventVisibilityKey,
		Category:     taskEntity.EventCategoryRetry,
		Code:         code,
		SourceSchema: schema,
		SourceTable:  table,
		Message:      message,
		Details:      details,
	})
}

func effectiveFullLoadEngineLabel(cfg taskEntity.TaskConfig) string {
	if cfg.UsesFullLoadV2() {
		return "v2"
	}
	if cfg.FullLoadEngine != "" {
		return cfg.FullLoadEngine
	}
	return "v1"
}
