package port

import (
	"time"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
)

// TaskEventListFilter 事件列表查询条件。
type TaskEventListFilter struct {
	TaskID       string
	ExecutionID  string
	AfterSeq     int64
	BeforeSeq    int64
	Limit        int
	MinSeverity  taskEntity.EventSeverity
	Visibility   taskEntity.EventVisibility
	Category     taskEntity.EventCategory
	Code         string
	SourceSchema string
	SourceTable  string
}

// TaskEventStore 任务事件持久化接口。
type TaskEventStore interface {
	Append(event *taskEntity.TaskEvent) error
	List(filter TaskEventListFilter) ([]*taskEntity.TaskEvent, error)
	ListExecutions(taskID string) ([]*taskEntity.TaskEventExecution, error)
	DeleteByTask(taskID string) error
	Prune(taskID string, opts TaskEventPruneOptions) (int, error)
}

// TaskEventPruneOptions 事件保留策略。
type TaskEventPruneOptions struct {
	MaxKeyEvents      int
	MaxAge            time.Duration
	MinErrorEvents    int
	CurrentExecution  string
	ProtectExecutions map[string]struct{}
}
