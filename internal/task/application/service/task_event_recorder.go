package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
	"mysql-to-sync/internal/metrics"
	"mysql-to-sync/pkg/logger"
	"mysql-to-sync/pkg/taskevent"
)

const (
	eventAggregationWindow   = 60 * time.Second
	eventSyncFallbackDelay   = 2 * time.Second
	defaultEventQueueSize    = 4096
	defaultCriticalQueueSize = 512
)

// eventAggWindow 可在测试中临时缩短聚合窗口。
var eventAggWindow = eventAggregationWindow

type pendingEmit struct {
	event *taskEntity.TaskEvent
}

type aggregateBucket struct {
	first    *taskEntity.TaskEvent
	lastAt   time.Time
	count    int
	flushAt  time.Time
}

// TaskEventRecorder 统一事件 Emit 入口：持久化 + 日志镜像 + 指纹聚合。
type TaskEventRecorder struct {
	store port.TaskEventStore

	queue chan pendingEmit

	mu           sync.Mutex
	executions   map[string]string
	aggregating  map[string]*aggregateBucket
	deletedTasks map[string]struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewTaskEventRecorder 创建 recorder 并启动异步写入 goroutine。
func NewTaskEventRecorder(store port.TaskEventStore) *TaskEventRecorder {
	if store == nil {
		return nil
	}
	r := &TaskEventRecorder{
		store:        store,
		queue:        make(chan pendingEmit, defaultEventQueueSize),
		executions:   make(map[string]string),
		aggregating:  make(map[string]*aggregateBucket),
		deletedTasks: make(map[string]struct{}),
		stopCh:       make(chan struct{}),
	}
	r.wg.Add(1)
	go r.worker()
	return r
}

// Close 停止 worker 并刷出聚合窗口。
func (r *TaskEventRecorder) Close() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
	r.wg.Wait()
}

// SetExecutionID 绑定任务当前 execution（StartTask / executeSync 时调用）。
func (r *TaskEventRecorder) SetExecutionID(taskID, executionID string) {
	if r == nil || taskID == "" || executionID == "" {
		return
	}
	r.mu.Lock()
	r.executions[taskID] = executionID
	r.mu.Unlock()
}

// CurrentExecutionID 返回任务当前 execution。
func (r *TaskEventRecorder) CurrentExecutionID(taskID string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.executions[taskID]
}

// Emit 写入事件；KEY 可见性事件进入 store，同时镜像 logger。
func (r *TaskEventRecorder) Emit(ev taskEntity.TaskEvent) {
	if r == nil {
		return
	}
	if ev.TaskID == "" {
		return
	}
	r.mu.Lock()
	if ev.ExecutionID == "" {
		ev.ExecutionID = r.executions[ev.TaskID]
	}
	r.mu.Unlock()
	if ev.ExecutionID == "" {
		return
	}
	if ev.EventID == "" {
		ev.EventID = uuid.NewString()
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	if ev.Visibility == "" {
		ev.Visibility = taskEntity.EventVisibilityKey
	}
	if err := taskEntity.ValidateTaskEvent(&ev); err != nil {
		logger.Warn("[Task %s] invalid task event %s: %v", ev.TaskID, ev.Code, err)
		return
	}

	ev.Message, ev.Details = taskevent.SanitizeTaskEventFields(ev.Message, ev.Details)
	r.mirrorLogger(ev)

	if taskEntity.IsNeverSuppressEventCode(ev.Code) {
		r.enqueue(ev, true)
		return
	}
	if r.tryAggregate(ev) {
		return
	}
	r.enqueue(ev, ev.Severity == taskEntity.EventSeverityWarn || ev.Severity == taskEntity.EventSeverityError)
}

func (r *TaskEventRecorder) fingerprint(ev taskEntity.TaskEvent) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|%s",
		ev.TaskID, ev.ExecutionID, ev.Code, ev.Severity, ev.SourceSchema, ev.SourceTable, ev.Message)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func (r *TaskEventRecorder) tryAggregate(ev taskEntity.TaskEvent) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	fp := r.fingerprint(ev)
	b, ok := r.aggregating[fp]
	now := ev.Timestamp
	if !ok {
		copyEv := ev
		r.aggregating[fp] = &aggregateBucket{
			first:   &copyEv,
			lastAt:  now,
			count:   1,
			flushAt: now.Add(eventAggWindow),
		}
		return true
	}
	b.count++
	b.lastAt = now
	return true
}

func (r *TaskEventRecorder) flushExpiredAggregates() {
	r.flushAggregates(false)
}

func (r *TaskEventRecorder) flushAllAggregates() {
	r.flushAggregates(true)
}

func (r *TaskEventRecorder) flushAggregates(forceAll bool) {
	r.mu.Lock()
	var flush []*taskEntity.TaskEvent
	now := time.Now()
	for fp, b := range r.aggregating {
		if !forceAll && now.Before(b.flushAt) {
			continue
		}
		if b.count <= 1 {
			flush = append(flush, b.first)
		} else {
			summary := *b.first
			summary.Code = b.first.Code + "_REPEATED"
			summary.RepeatCount = b.count
			firstAt := b.first.Timestamp
			summary.FirstAt = &firstAt
			lastAt := b.lastAt
			summary.LastAt = &lastAt
			summary.Message = fmt.Sprintf("%s (repeated %d times in %s)", b.first.Message, b.count, eventAggWindow)
			flush = append(flush, &summary)
		}
		delete(r.aggregating, fp)
	}
	r.mu.Unlock()
	for _, ev := range flush {
		r.enqueue(*ev, ev.Severity == taskEntity.EventSeverityWarn || ev.Severity == taskEntity.EventSeverityError)
	}
}

func (r *TaskEventRecorder) isTaskDeleted(taskID string) bool {
	if r == nil || taskID == "" {
		return false
	}
	r.mu.Lock()
	_, deleted := r.deletedTasks[taskID]
	r.mu.Unlock()
	return deleted
}

func (r *TaskEventRecorder) enqueue(ev taskEntity.TaskEvent, critical bool) {
	if r.isTaskDeleted(ev.TaskID) {
		return
	}
	evCopy := ev
	select {
	case r.queue <- pendingEmit{event: &evCopy}:
		return
	default:
		if critical {
			select {
			case r.queue <- pendingEmit{event: &evCopy}:
				return
			default:
			}
			if err := r.store.Append(&evCopy); err != nil {
				logger.Warn("[Task %s] sync task event append failed code=%s: %v", ev.TaskID, ev.Code, err)
			}
			return
		}
		metrics.GetMetrics().IncrementTaskEventDropped()
	}
}

func (r *TaskEventRecorder) syncAppendWithRetry(ev taskEntity.TaskEvent) {
	if r.isTaskDeleted(ev.TaskID) {
		return
	}
	time.Sleep(eventSyncFallbackDelay)
	if r.isTaskDeleted(ev.TaskID) {
		return
	}
	if err := r.store.Append(&ev); err != nil {
		logger.Warn("[Task %s] sync task event append failed code=%s: %v", ev.TaskID, ev.Code, err)
	}
}

func (r *TaskEventRecorder) worker() {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			r.flushAllAggregates()
			for {
				select {
				case job := <-r.queue:
					if job.event != nil && !r.isTaskDeleted(job.event.TaskID) {
						_ = r.store.Append(job.event)
					}
				default:
					return
				}
			}
		case job := <-r.queue:
			if job.event != nil {
				if r.isTaskDeleted(job.event.TaskID) {
					continue
				}
				if err := r.store.Append(job.event); err != nil {
					logger.Warn("[Task %s] async task event append failed code=%s: %v", job.event.TaskID, job.event.Code, err)
					go r.syncAppendWithRetry(*job.event)
				}
			}
		case <-ticker.C:
			r.flushExpiredAggregates()
		}
	}
}

func (r *TaskEventRecorder) mirrorLogger(ev taskEntity.TaskEvent) {
	prefix := fmt.Sprintf("[Task %s][Event %s]", ev.TaskID, ev.Code)
	switch ev.Severity {
	case taskEntity.EventSeverityError:
		logger.Error("%s %s", prefix, ev.Message)
	case taskEntity.EventSeverityWarn:
		logger.Warn("%s %s", prefix, ev.Message)
	default:
		if ev.Visibility == taskEntity.EventVisibilityKey {
			logger.Info("%s %s", prefix, ev.Message)
		}
	}
}

// EmitLifecycle 便捷方法：生命周期事件。
func (r *TaskEventRecorder) EmitLifecycle(taskID, code, message string, severity taskEntity.EventSeverity) {
	if r == nil {
		return
	}
	r.Emit(taskEntity.TaskEvent{
		TaskID:     taskID,
		Severity:   severity,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryLifecycle,
		Code:       code,
		Message:    message,
	})
}

// EmitPhase 便捷方法：阶段事件。
func (r *TaskEventRecorder) EmitPhase(taskID, code, phase, message string) {
	if r == nil {
		return
	}
	r.Emit(taskEntity.TaskEvent{
		TaskID:     taskID,
		Severity:   taskEntity.EventSeverityInfo,
		Visibility: taskEntity.EventVisibilityKey,
		Category:   taskEntity.EventCategoryPhase,
		Code:       code,
		Phase:      phase,
		Message:    message,
	})
}

// ListEvents 代理 store 查询。
func (r *TaskEventRecorder) ListEvents(filter port.TaskEventListFilter) ([]*taskEntity.TaskEvent, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("task event recorder not configured")
	}
	return r.store.List(filter)
}

// ListExecutions 代理 store 查询。
func (r *TaskEventRecorder) ListExecutions(taskID string) ([]*taskEntity.TaskEventExecution, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("task event recorder not configured")
	}
	return r.store.ListExecutions(taskID)
}

// DeleteByTask 删除任务全部事件。
func (r *TaskEventRecorder) DeleteByTask(taskID string) error {
	if r == nil || r.store == nil {
		return nil
	}
	r.mu.Lock()
	delete(r.executions, taskID)
	r.deletedTasks[taskID] = struct{}{}
	for fp, b := range r.aggregating {
		if b.first != nil && b.first.TaskID == taskID {
			delete(r.aggregating, fp)
		}
	}
	r.mu.Unlock()
	return r.store.DeleteByTask(taskID)
}

// PruneTask 清理单任务事件。
func (r *TaskEventRecorder) PruneTask(taskID string, opts port.TaskEventPruneOptions) (int, error) {
	if r == nil || r.store == nil {
		return 0, nil
	}
	return r.store.Prune(taskID, opts)
}
