package storage // 声明当前文件属于storage包，用于存储功能

import ( // 导入外部包
	"encoding/json" // 导入encoding/json包，用于JSON编解码
	"os" // 导入os包，用于操作系统接口
	"path/filepath" // 导入path/filepath包，用于文件路径操作
	"sync" // 导入sync包，用于并发控制

	taskEntity "mysql-to-async/internal/task/domain/entity" // 导入任务实体
)

// Storage 存储接口定义
type Storage interface { // 定义存储接口
	Save(task *taskEntity.SyncTask) error // 保存任务方法
	Load(taskID string) (*taskEntity.SyncTask, error) // 加载任务方法
	Delete(taskID string) error // 删除任务方法
	ListTasks() ([]*taskEntity.SyncTask, error) // 列出任务方法
}

// TaskStorage 任务存储结构体
type TaskStorage struct { // 定义任务存储结构体
	mu       sync.RWMutex // 读写互斥锁，用于保证线程安全
	dataFile string // 数据文件路径
	dataDir  string // 数据目录路径
	tasks    map[string]*taskEntity.SyncTask // 任务映射表，键为任务ID
}

// NewTaskStorage 创建新的任务存储函数
func NewTaskStorage(dataDir string) *TaskStorage { // 创建新的任务存储实例
	if dataDir == "" { // 如果未指定数据目录
		dataDir = "data" // 使用默认数据目录
	}

	ts := &TaskStorage{ // 创建TaskStorage实例
		dataDir:  dataDir, // 设置数据目录
		dataFile: filepath.Join(dataDir, "tasks.json"), // 设置数据文件路径
		tasks:    make(map[string]*taskEntity.SyncTask), // 初始化任务映射表
	}

	// 确保目录存在
	os.MkdirAll(dataDir, 0755) // 创建数据目录

	// 加载已有任务
	ts.loadFromFile() // 从文件加载任务

	return ts // 返回任务存储实例
}

// loadFromFile 从文件加载任务方法
func (s *TaskStorage) loadFromFile() error { // 从文件加载任务数据
	data, err := os.ReadFile(s.dataFile) // 读取数据文件
	if err != nil { // 如果读取失败
		if os.IsNotExist(err) { // 如果文件不存在
			return nil // 返回nil，表示无数据
		}
		return err // 返回错误
	}

	var tasks []*taskEntity.SyncTask // 定义任务列表变量
	if err := json.Unmarshal(data, &tasks); err != nil { // 解析JSON数据
		return err // 返回解析错误
	}

	for _, task := range tasks { // 遍历任务列表
		s.tasks[task.Config.ID] = task // 将任务存入映射表
	}

	return nil // 返回nil表示成功
}

// Save 保存任务方法
func (s *TaskStorage) Save(task *taskEntity.SyncTask) error { // 保存任务到存储
	s.mu.Lock() // 获取写锁
	defer s.mu.Unlock() // 延迟释放锁

	s.tasks[task.Config.ID] = task // 将任务存入映射表

	// 保存到文件
	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks)) // 创建任务列表
	for _, t := range s.tasks { // 遍历任务映射表
		tasks = append(tasks, t) // 将任务添加到列表
	}

	data, err := json.MarshalIndent(tasks, "", "  ") // 将任务列表序列化为JSON
	if err != nil { // 如果序列化失败
		return err // 返回错误
	}

	return os.WriteFile(s.dataFile, data, 0644) // 写入文件
}

// Load 加载任务方法
func (s *TaskStorage) Load(taskID string) (*taskEntity.SyncTask, error) { // 根据ID加载任务
	s.mu.RLock() // 获取读锁
	defer s.mu.RUnlock() // 延迟释放锁

	task, exists := s.tasks[taskID] // 从映射表获取任务
	if !exists { // 如果任务不存在
		return nil, os.ErrNotExist // 返回不存在错误
	}

	return task, nil // 返回任务和nil
}

// Delete 删除任务方法
func (s *TaskStorage) Delete(taskID string) error { // 根据ID删除任务
	s.mu.Lock() // 获取写锁
	defer s.mu.Unlock() // 延迟释放锁

	if _, exists := s.tasks[taskID]; !exists { // 检查任务是否存在
		return os.ErrNotExist // 返回不存在错误
	}

	delete(s.tasks, taskID) // 从映射表删除任务

	// 保存到文件
	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks)) // 创建任务列表
	for _, t := range s.tasks { // 遍历任务映射表
		tasks = append(tasks, t) // 将任务添加到列表
	}

	data, err := json.MarshalIndent(tasks, "", "  ") // 将任务列表序列化为JSON
	if err != nil { // 如果序列化失败
		return err // 返回错误
	}

	return os.WriteFile(s.dataFile, data, 0644) // 写入文件
}

// ListTasks 列出所有任务方法
func (s *TaskStorage) ListTasks() ([]*taskEntity.SyncTask, error) { // 获取所有任务列表
	s.mu.RLock() // 获取读锁
	defer s.mu.RUnlock() // 延迟释放锁

	tasks := make([]*taskEntity.SyncTask, 0, len(s.tasks)) // 创建任务列表
	for _, t := range s.tasks { // 遍历任务映射表
		tasks = append(tasks, t) // 将任务添加到列表
	}

	return tasks, nil // 返回任务列表和nil
}

// GetTask 获取任务方法
func (s *TaskStorage) GetTask(id string) (*taskEntity.SyncTask, bool) { // 根据ID获取任务
	s.mu.RLock() // 获取读锁
	defer s.mu.RUnlock() // 延迟释放锁

	task, exists := s.tasks[id] // 从映射表获取任务
	return task, exists // 返回任务和存在标志
}
