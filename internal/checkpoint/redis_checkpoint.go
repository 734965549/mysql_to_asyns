package checkpoint // 声明当前文件属于checkpoint包，用于检查点管理

import ( // 导入外部包和标准库
	"context" // 导入context包，用于上下文管理
	"encoding/json" // 导入encoding/json包，用于JSON编码解码
	"fmt" // 导入fmt包，用于格式化输入输出
	"time" // 导入time包，用于时间处理

	"github.com/go-mysql-org/go-mysql/mysql" // 导入MySQL库
	"github.com/redis/go-redis/v9" // 导入Redis客户端库
)

// Checkpoint 增量同步位点信息（Binlog 文件/位置、按表 Offset 等）。
//
// 注意：全量同步的行级/表级断点续传保存在任务存档 ProcessContext.FullSyncResume 中，
// 不由本包的 Manager 管理。SavePosition/GetPosition 主要用于增量阶段的 binlog 订阅恢复。
type Checkpoint struct { // 定义检查点结构体
	TaskID        string    `json:"task_id"` // 任务ID
	TableName     string    `json:"table_name"` // 表名
	Schema        string    `json:"schema"` // 模式名
	BinlogFile    string    `json:"binlog_file"` // Binlog文件名
	BinlogPos     uint32    `json:"binlog_pos"` // Binlog位置
	Offset        int64     `json:"offset"` // 全量同步偏移量
	ProcessedRows int64     `json:"processed_rows"` // 已处理行数
	LastUpdate    time.Time `json:"last_update"` // 最后更新时间
}

// Manager 位点管理器接口
type Manager interface { // 定义检查点管理器接口
	// Save 保存位点方法
	Save(ctx context.Context, cp *Checkpoint) error // 保存检查点
	// Get 获取位点方法
	Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error) // 获取指定表的检查点
	// GetAll 获取任务所有表的位点方法
	GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error) // 获取任务所有表的检查点
	// Delete 删除位点方法
	Delete(ctx context.Context, taskID string) error // 删除任务的所有检查点
	// GetPosition 获取binlog位置方法
	GetPosition(ctx context.Context, taskID string) (mysql.Position, error) // 获取binlog位置
	// SavePosition 保存binlog位置方法
	SavePosition(ctx context.Context, taskID string, pos mysql.Position) error // 保存binlog位置
}

// RedisCheckpointManager Redis位点管理器结构体
type RedisCheckpointManager struct { // 定义Redis检查点管理器
	client *redis.Client // Redis客户端
	prefix string // 键前缀
}

// NewRedisCheckpointManager 创建Redis位点管理器函数
func NewRedisCheckpointManager(client *redis.Client, prefix string) *RedisCheckpointManager { // 创建Redis检查点管理器
	if prefix == "" { // 如果前缀为空
		prefix = "dts:checkpoint" // 使用默认前缀
	}
	return &RedisCheckpointManager{ // 返回Redis检查点管理器实例
		client: client, // 设置Redis客户端
		prefix: prefix, // 设置键前缀
	}
}

// getKey 生成Redis键方法
func (m *RedisCheckpointManager) getKey(taskID, tableName string) string { // 生成表检查点的Redis键
	return fmt.Sprintf("%s:%s:%s", m.prefix, taskID, tableName) // 格式化生成键
}

// getTaskKey 生成任务键方法
func (m *RedisCheckpointManager) getTaskKey(taskID string) string { // 生成任务相关的Redis键
	return fmt.Sprintf("%s:%s", m.prefix, taskID) // 格式化生成键
}

// getPositionKey 生成位置键方法
func (m *RedisCheckpointManager) getPositionKey(taskID string) string { // 生成binlog位置的Redis键
	return fmt.Sprintf("%s:%s:position", m.prefix, taskID) // 格式化生成键
}

// Save 保存位点方法
func (m *RedisCheckpointManager) Save(ctx context.Context, cp *Checkpoint) error { // 保存检查点到Redis
	cp.LastUpdate = time.Now() // 设置最后更新时间
	data, err := json.Marshal(cp) // 序列化检查点
	if err != nil { // 如果序列化失败
		return fmt.Errorf("failed to marshal checkpoint: %w", err) // 返回错误
	}

	key := m.getKey(cp.TaskID, cp.TableName) // 生成Redis键
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil { // 保存到Redis，永不过期
		return fmt.Errorf("failed to save checkpoint: %w", err) // 返回错误
	}

	return nil // 返回成功
}

// Get 获取位点方法
func (m *RedisCheckpointManager) Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error) { // 从Redis获取检查点
	key := m.getKey(taskID, tableName) // 生成Redis键
	data, err := m.client.Get(ctx, key).Result() // 从Redis获取数据
	if err == redis.Nil { // 如果键不存在
		return nil, nil // 返回nil表示没有找到
	}
	if err != nil { // 如果获取失败
		return nil, fmt.Errorf("failed to get checkpoint: %w", err) // 返回错误
	}

	var cp Checkpoint // 定义检查点变量
	if err := json.Unmarshal([]byte(data), &cp); err != nil { // 反序列化检查点
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err) // 返回错误
	}

	return &cp, nil // 返回检查点
}

// GetAll 获取任务所有表的位点方法
func (m *RedisCheckpointManager) GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error) { // 获取任务所有表的检查点
	pattern := m.getKey(taskID, "*") // 生成键模式
	keys, err := m.client.Keys(ctx, pattern).Result() // 查询匹配的键
	if err != nil { // 如果查询失败
		return nil, fmt.Errorf("failed to list keys: %w", err) // 返回错误
	}

	checkpoints := make(map[string]*Checkpoint) // 创建检查点映射
	for _, key := range keys { // 遍历所有键
		data, err := m.client.Get(ctx, key).Result() // 获取检查点数据
		if err != nil { // 如果获取失败
			continue // 跳过
		}

		var cp Checkpoint // 定义检查点变量
		if err := json.Unmarshal([]byte(data), &cp); err != nil { // 反序列化检查点
			continue // 跳过
		}
		checkpoints[cp.TableName] = &cp // 添加到映射
	}

	return checkpoints, nil // 返回检查点映射
}

// Delete 删除位点方法
func (m *RedisCheckpointManager) Delete(ctx context.Context, taskID string) error { // 删除任务的所有检查点
	pattern := m.getKey(taskID, "*") // 生成键模式
	keys, err := m.client.Keys(ctx, pattern).Result() // 查询匹配的键
	if err != nil { // 如果查询失败
		return fmt.Errorf("failed to list keys: %w", err) // 返回错误
	}

	if len(keys) > 0 { // 如果有匹配的键
		if err := m.client.Del(ctx, keys...).Err(); err != nil { // 删除所有匹配的键
			return fmt.Errorf("failed to delete checkpoints: %w", err) // 返回错误
		}
	}

	// 删除位置键
	posKey := m.getPositionKey(taskID) // 生成位置键
	m.client.Del(ctx, posKey) // 删除位置键

	return nil // 返回成功
}

// GetPosition 获取binlog位置方法
func (m *RedisCheckpointManager) GetPosition(ctx context.Context, taskID string) (mysql.Position, error) { // 获取binlog位置
	key := m.getPositionKey(taskID) // 生成位置键
	data, err := m.client.Get(ctx, key).Result() // 获取位置数据
	if err == redis.Nil { // 如果键不存在
		return mysql.Position{}, nil // 返回空位置
	}
	if err != nil { // 如果获取失败
		return mysql.Position{}, fmt.Errorf("failed to get position: %w", err) // 返回错误
	}

	var pos struct { // 定义位置结构体
		Name string `json:"name"` // 文件名
		Pos  uint32 `json:"pos"` // 位置
	}
	if err := json.Unmarshal([]byte(data), &pos); err != nil { // 反序列化位置数据
		return mysql.Position{}, fmt.Errorf("failed to unmarshal position: %w", err) // 返回错误
	}

	return mysql.Position{ // 返回位置对象
		Name: pos.Name, // 设置文件名
		Pos:  pos.Pos, // 设置位置
	}, nil
}

// SavePosition 保存binlog位置方法
func (m *RedisCheckpointManager) SavePosition(ctx context.Context, taskID string, pos mysql.Position) error { // 保存binlog位置
	data, err := json.Marshal(struct { // 序列化位置数据
		Name string `json:"name"` // 文件名
		Pos  uint32 `json:"pos"` // 位置
	}{
		Name: pos.Name, // 设置文件名
		Pos:  pos.Pos, // 设置位置
	})
	if err != nil { // 如果序列化失败
		return fmt.Errorf("failed to marshal position: %w", err) // 返回错误
	}

	key := m.getPositionKey(taskID) // 生成位置键
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil { // 保存到Redis
		return fmt.Errorf("failed to save position: %w", err) // 返回错误
	}

	return nil // 返回成功
}

// MemoryCheckpointManager 内存位点管理器结构体（用于测试或无Redis场景）
type MemoryCheckpointManager struct { // 定义内存检查点管理器
	checkpoints map[string]*Checkpoint // 检查点映射
	positions   map[string]mysql.Position // 位置映射
}

// NewMemoryCheckpointManager 创建内存位点管理器函数
func NewMemoryCheckpointManager() *MemoryCheckpointManager { // 创建内存检查点管理器
	return &MemoryCheckpointManager{ // 返回内存检查点管理器实例
		checkpoints: make(map[string]*Checkpoint), // 初始化检查点映射
		positions:   make(map[string]mysql.Position), // 初始化位置映射
	}
}

// getKey 生成键方法
func (m *MemoryCheckpointManager) getKey(taskID, tableName string) string { // 生成内存键
	return fmt.Sprintf("%s:%s", taskID, tableName) // 格式化生成键
}

// Save 保存位点方法
func (m *MemoryCheckpointManager) Save(ctx context.Context, cp *Checkpoint) error { // 保存检查点到内存
	cp.LastUpdate = time.Now() // 设置最后更新时间
	key := m.getKey(cp.TaskID, cp.TableName) // 生成键
	m.checkpoints[key] = cp // 保存到映射
	return nil // 返回成功
}

// Get 获取位点方法
func (m *MemoryCheckpointManager) Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error) { // 从内存获取检查点
	key := m.getKey(taskID, tableName) // 生成键
	return m.checkpoints[key], nil // 返回检查点
}

// GetAll 获取任务所有表的位点方法
func (m *MemoryCheckpointManager) GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error) { // 获取任务所有表的检查点
	result := make(map[string]*Checkpoint) // 创建结果映射
	for k, v := range m.checkpoints { // 遍历所有检查点
		if len(k) > len(taskID)+1 && k[:len(taskID)] == taskID { // 如果键以任务ID开头
			result[v.TableName] = v // 添加到结果
		}
	}
	return result, nil // 返回结果
}

// Delete 删除位点方法
func (m *MemoryCheckpointManager) Delete(ctx context.Context, taskID string) error { // 删除任务的所有检查点
	for k := range m.checkpoints { // 遍历所有检查点键
		if len(k) > len(taskID)+1 && k[:len(taskID)] == taskID { // 如果键以任务ID开头
			delete(m.checkpoints, k) // 删除检查点
		}
	}
	delete(m.positions, taskID) // 删除位置
	return nil // 返回成功
}

// GetPosition 获取binlog位置方法
func (m *MemoryCheckpointManager) GetPosition(ctx context.Context, taskID string) (mysql.Position, error) { // 获取binlog位置
	return m.positions[taskID], nil // 返回位置
}

// SavePosition 保存binlog位置方法
func (m *MemoryCheckpointManager) SavePosition(ctx context.Context, taskID string, pos mysql.Position) error { // 保存binlog位置
	m.positions[taskID] = pos // 保存到映射
	return nil // 返回成功
}
