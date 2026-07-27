package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/internal/task/domain/port"
)

const (
	defaultEventFileMaxBytes = 32 << 20 // 32 MiB 后轮转
)

// FileTaskEventStore 基于 JSONL 的任务事件存储。
type FileTaskEventStore struct {
	baseDir string
	mu      sync.Mutex
	locks   map[string]*sync.Mutex
	seq     map[string]int64
}

// NewFileTaskEventStore 创建文件事件存储；baseDir 通常为 data_dir/task-events。
func NewFileTaskEventStore(baseDir string) (*FileTaskEventStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create task-events dir: %w", err)
	}
	return &FileTaskEventStore{
		baseDir: baseDir,
		locks:   make(map[string]*sync.Mutex),
		seq:     make(map[string]int64),
	}, nil
}

func safeTaskEventBasename(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return hex.EncodeToString(sum[:16])
}

func (s *FileTaskEventStore) eventPath(taskID string) string {
	return filepath.Join(s.baseDir, safeTaskEventBasename(taskID)+".jsonl")
}

func (s *FileTaskEventStore) eventDataPaths(taskID string) ([]string, error) {
	base := s.eventPath(taskID)
	var paths []string
	if _, err := os.Stat(base); err == nil {
		paths = append(paths, base)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	matches, err := filepath.Glob(base + ".*.bak")
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	paths = append(paths, matches...)
	return paths, nil
}

func (s *FileTaskEventStore) removeBackupFiles(taskID string) {
	pattern := s.eventPath(taskID) + ".*.bak"
	matches, _ := filepath.Glob(pattern)
	for _, p := range matches {
		_ = os.Remove(p)
	}
}

func (s *FileTaskEventStore) loadEventsFromPath(path string) ([]*taskEntity.TaskEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var out []*taskEntity.TaskEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev taskEntity.TaskEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		out = append(out, &ev)
	}
	return out, scanner.Err()
}

func (s *FileTaskEventStore) taskLock(taskID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[taskID] == nil {
		s.locks[taskID] = &sync.Mutex{}
	}
	return s.locks[taskID]
}

func (s *FileTaskEventStore) recoverSeqLocked(taskID string) error {
	if _, ok := s.seq[taskID]; ok {
		return nil
	}
	paths, err := s.eventDataPaths(taskID)
	if err != nil {
		return err
	}
	var maxSeq int64
	for _, path := range paths {
		events, err := s.loadEventsFromPath(path)
		if err != nil {
			return err
		}
		for _, ev := range events {
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
		}
	}
	s.seq[taskID] = maxSeq
	return nil
}

func (s *FileTaskEventStore) nextSeq(taskID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.recoverSeqLocked(taskID); err != nil {
		return 0, err
	}
	s.seq[taskID]++
	return s.seq[taskID], nil
}

func (s *FileTaskEventStore) rotateIfNeeded(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() < defaultEventFileMaxBytes {
		return nil
	}
	rotated := path + "." + time.Now().UTC().Format("20060102-150405") + ".bak"
	return os.Rename(path, rotated)
}

// Append 追加一条事件。
func (s *FileTaskEventStore) Append(event *taskEntity.TaskEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if err := taskEntity.ValidateTaskEvent(event); err != nil {
		return err
	}
	lock := s.taskLock(event.TaskID)
	lock.Lock()
	defer lock.Unlock()

	seq, err := s.nextSeq(event.TaskID)
	if err != nil {
		return err
	}
	event.Seq = seq

	path := s.eventPath(event.TaskID)
	if err := s.rotateIfNeeded(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (s *FileTaskEventStore) loadAll(taskID string) ([]*taskEntity.TaskEvent, error) {
	paths, err := s.eventDataPaths(taskID)
	if err != nil {
		return nil, err
	}
	var out []*taskEntity.TaskEvent
	for _, path := range paths {
		events, err := s.loadEventsFromPath(path)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	return out, nil
}

func matchEventFilter(ev *taskEntity.TaskEvent, f port.TaskEventListFilter) bool {
	if f.ExecutionID != "" && ev.ExecutionID != f.ExecutionID {
		return false
	}
	if f.AfterSeq > 0 && ev.Seq <= f.AfterSeq {
		return false
	}
	if f.BeforeSeq > 0 && ev.Seq >= f.BeforeSeq {
		return false
	}
	if f.MinSeverity != "" && taskEntity.SeverityRank(ev.Severity) < taskEntity.SeverityRank(f.MinSeverity) {
		return false
	}
	if f.Visibility != "" && ev.Visibility != f.Visibility {
		return false
	}
	if f.Category != "" && ev.Category != f.Category {
		return false
	}
	if f.Code != "" && ev.Code != f.Code {
		return false
	}
	if f.SourceSchema != "" && ev.SourceSchema != f.SourceSchema {
		return false
	}
	if f.SourceTable != "" && ev.SourceTable != f.SourceTable {
		return false
	}
	return true
}

// List 按条件列出事件（默认最新在前）。
func (s *FileTaskEventStore) List(filter port.TaskEventListFilter) ([]*taskEntity.TaskEvent, error) {
	if filter.TaskID == "" {
		return nil, fmt.Errorf("task_id is required")
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	all, err := s.loadAll(filter.TaskID)
	if err != nil {
		return nil, err
	}
	var matched []*taskEntity.TaskEvent
	for _, ev := range all {
		if matchEventFilter(ev, filter) {
			matched = append(matched, ev)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].Seq > matched[j].Seq
	})
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

// ListExecutions 列出任务的 execution 摘要。
func (s *FileTaskEventStore) ListExecutions(taskID string) ([]*taskEntity.TaskEventExecution, error) {
	all, err := s.loadAll(taskID)
	if err != nil {
		return nil, err
	}
	type agg struct {
		start time.Time
		end   time.Time
		count int
	}
	m := make(map[string]*agg)
	for _, ev := range all {
		a, ok := m[ev.ExecutionID]
		if !ok {
			m[ev.ExecutionID] = &agg{start: ev.Timestamp, end: ev.Timestamp, count: 1}
			continue
		}
		if ev.Timestamp.Before(a.start) {
			a.start = ev.Timestamp
		}
		if ev.Timestamp.After(a.end) {
			a.end = ev.Timestamp
		}
		a.count++
	}
	out := make([]*taskEntity.TaskEventExecution, 0, len(m))
	for id, a := range m {
		end := a.end
		out = append(out, &taskEntity.TaskEventExecution{
			ExecutionID: id,
			StartedAt:   a.start,
			EndedAt:     &end,
			EventCount:  a.count,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.After(out[j].StartedAt)
	})
	return out, nil
}

// DeleteByTask 删除任务全部事件文件。
func (s *FileTaskEventStore) DeleteByTask(taskID string) error {
	lock := s.taskLock(taskID)
	lock.Lock()
	defer lock.Unlock()

	path := s.eventPath(taskID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	// 清理轮转备份
	pattern := path + ".*.bak"
	matches, _ := filepath.Glob(pattern)
	for _, p := range matches {
		_ = os.Remove(p)
	}
	s.mu.Lock()
	delete(s.seq, taskID)
	delete(s.locks, taskID)
	s.mu.Unlock()
	return nil
}

// Prune 按保留策略重写事件文件。
func (s *FileTaskEventStore) Prune(taskID string, opts port.TaskEventPruneOptions) (int, error) {
	lock := s.taskLock(taskID)
	lock.Lock()
	defer lock.Unlock()

	all, err := s.loadAll(taskID)
	if err != nil {
		return 0, err
	}
	if len(all) == 0 {
		return 0, nil
	}
	keep := selectEventsToKeep(all, opts)
	if len(keep) == len(all) {
		return 0, nil
	}
	removed := len(all) - len(keep)

	path := s.eventPath(taskID)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	sort.Slice(keep, func(i, j int) bool { return keep[i].Seq < keep[j].Seq })
	var maxSeq int64
	for _, ev := range keep {
		data, err := json.Marshal(ev)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return 0, err
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return 0, err
		}
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return 0, err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	s.mu.Lock()
	s.seq[taskID] = maxSeq
	s.mu.Unlock()
	s.removeBackupFiles(taskID)
	return removed, nil
}

func selectEventsToKeep(all []*taskEntity.TaskEvent, opts port.TaskEventPruneOptions) []*taskEntity.TaskEvent {
	if len(all) == 0 {
		return nil
	}
	cutoff := time.Time{}
	if opts.MaxAge > 0 {
		cutoff = time.Now().Add(-opts.MaxAge)
	}
	protected := opts.ProtectExecutions
	if protected == nil {
		protected = make(map[string]struct{})
	}
	if opts.CurrentExecution != "" {
		protected[opts.CurrentExecution] = struct{}{}
	}

	var keyEvents, errorEvents []*taskEntity.TaskEvent
	for _, ev := range all {
		if _, ok := protected[ev.ExecutionID]; ok {
			continue
		}
		if !cutoff.IsZero() && ev.Timestamp.Before(cutoff) && ev.Visibility == taskEntity.EventVisibilityKey {
			continue
		}
		if ev.Severity == taskEntity.EventSeverityError {
			errorEvents = append(errorEvents, ev)
		}
		if ev.Visibility == taskEntity.EventVisibilityKey {
			keyEvents = append(keyEvents, ev)
		}
	}
	sort.Slice(keyEvents, func(i, j int) bool { return keyEvents[i].Seq > keyEvents[j].Seq })
	maxKey := opts.MaxKeyEvents
	if maxKey <= 0 {
		maxKey = 2000
	}
	if len(keyEvents) > maxKey {
		keyEvents = keyEvents[:maxKey]
	}
	minErr := opts.MinErrorEvents
	if minErr <= 0 {
		minErr = 200
	}
	sort.Slice(errorEvents, func(i, j int) bool { return errorEvents[i].Seq > errorEvents[j].Seq })
	if len(errorEvents) > minErr {
		errorEvents = errorEvents[:minErr]
	}

	keepSet := make(map[int64]*taskEntity.TaskEvent)
	for _, ev := range all {
		if _, ok := protected[ev.ExecutionID]; ok {
			keepSet[ev.Seq] = ev
		}
	}
	for _, ev := range keyEvents {
		keepSet[ev.Seq] = ev
	}
	for _, ev := range errorEvents {
		keepSet[ev.Seq] = ev
	}
	out := make([]*taskEntity.TaskEvent, 0, len(keepSet))
	for _, ev := range keepSet {
		out = append(out, ev)
	}
	return out
}

var _ port.TaskEventStore = (*FileTaskEventStore)(nil)
