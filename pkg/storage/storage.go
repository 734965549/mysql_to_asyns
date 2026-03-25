package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	taskEntity "mysql-to-async/internal/task/domain/entity"
)

// Storage 存储接口
type Storage interface {
	Save(task *taskEntity.SyncTask) error
	Load(taskID string) (*taskEntity.SyncTask, error)
	Delete(taskID string) error
	ListTasks() ([]*taskEntity.SyncTask, error)
}

// TaskStorage 任务存储实现
type TaskStorage struct {
	mu       sync.RWMutex
	dataFile string
	dataDir  string
	tasks    map[string]*taskEntity.SyncTask
}

// NewTaskStorage 创建新的任务存储
func NewTaskStorage(dataDir string) *TaskStorage {
	if dataDir == "" {
		dataDir = "data"
	}

	ts := &TaskStorage{
		dataDir:  dataDir,
		dataFile: filepath.Join(dataDir, "tasks.json"),
		tasks:    make(map[string]*taskEntity.SyncTask),
	}

	// 确保目录存在
	os.MkdirAll(dataDir, 0755)

	// 加载已有任务
	ts.loadFromFile()

	return ts
}

// loadFromFile 从文件加载任务
func (s *TaskStorage) loadFromFile() error {
	data, err := os.ReadFile(s.dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var tasks []*taskEntity.SyncTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return err
	}

	for _, task := range tasks {
		s.tasks[task.Config.ID] = task
	}

	return nil
}

// Save 保存任务
func (s *TaskStorage) Save(task *taskEntity.SyncTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[task.Config.ID] = task

	// 保存到文件
	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.dataFile, data, 0644)
}

// Load 加载任务
func (s *TaskStorage) Load(taskID string) (*taskEntity.SyncTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, os.ErrNotExist
	}

	return task, nil
}

// Delete 删除任务
func (s *TaskStorage) Delete(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[taskID]; !exists {
		return os.ErrNotExist
	}

	delete(s.tasks, taskID)

	// 保存到文件
	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.dataFile, data, 0644)
}

// ListTasks 列出所有任务
func (s *TaskStorage) ListTasks() ([]*taskEntity.SyncTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

// GetTask 获取任务
func (s *TaskStorage) GetTask(id string) (*taskEntity.SyncTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[id]
	return task, exists
}