package service

import (
	"time"

	"mysql-to-sync/internal/config"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

// TaskEventPruneConfig 事件保留配置。
type TaskEventPruneConfig struct {
	MaxKeyEvents   int
	RetainDays     int
	MinErrorEvents int
	IntervalHours  int
}

// DefaultTaskEventPruneConfig 默认保留策略。
func DefaultTaskEventPruneConfig() TaskEventPruneConfig {
	return TaskEventPruneConfig{
		MaxKeyEvents:   2000,
		RetainDays:     30,
		MinErrorEvents: 200,
		IntervalHours:  24,
	}
}

func taskEventPruneConfigFrom(cfg *config.Config) TaskEventPruneConfig {
	out := DefaultTaskEventPruneConfig()
	if cfg == nil {
		return out
	}
	if cfg.TaskEvents.MaxKeyEvents > 0 {
		out.MaxKeyEvents = cfg.TaskEvents.MaxKeyEvents
	}
	if cfg.TaskEvents.RetainDays > 0 {
		out.RetainDays = cfg.TaskEvents.RetainDays
	}
	if cfg.TaskEvents.MinErrorEvents > 0 {
		out.MinErrorEvents = cfg.TaskEvents.MinErrorEvents
	}
	if cfg.TaskEvents.PruneHours > 0 {
		out.IntervalHours = cfg.TaskEvents.PruneHours
	}
	return out
}

// StartTaskEventPruneLoop 每日后台清理历史事件。
func (s *TaskService) StartTaskEventPruneLoop(cfg TaskEventPruneConfig) {
	if s == nil || s.eventRecorder == nil {
		return
	}
	if cfg.IntervalHours <= 0 {
		cfg.IntervalHours = 24
	}
	interval := time.Duration(cfg.IntervalHours) * time.Hour
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.pruneStop:
				return
			case <-ticker.C:
				s.pruneAllTaskEvents(cfg)
			}
		}
	}()
}

func (s *TaskService) pruneAllTaskEvents(cfg TaskEventPruneConfig) {
	s.mu.RLock()
	taskIDs := make([]string, 0, len(s.tasks))
	runningExec := make(map[string]string)
	for id, task := range s.tasks {
		taskIDs = append(taskIDs, id)
		if task.Context.Status == taskEntity.TaskStatusRunning {
			if exec := s.eventRecorder.CurrentExecutionID(id); exec != "" {
				runningExec[id] = exec
			}
		}
	}
	s.mu.RUnlock()

	maxAge := time.Duration(cfg.RetainDays) * 24 * time.Hour
	for _, taskID := range taskIDs {
		opts := port.TaskEventPruneOptions{
			MaxKeyEvents:   cfg.MaxKeyEvents,
			MaxAge:         maxAge,
			MinErrorEvents: cfg.MinErrorEvents,
			CurrentExecution: runningExec[taskID],
		}
		if _, err := s.eventRecorder.PruneTask(taskID, opts); err != nil {
			// 非致命
			continue
		}
	}
}
