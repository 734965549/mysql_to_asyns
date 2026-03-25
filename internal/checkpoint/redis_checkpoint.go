package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/redis/go-redis/v9"
)

// Checkpoint 位点信息
type Checkpoint struct {
	TaskID        string    `json:"task_id"`
	TableName     string    `json:"table_name"`
	Schema        string    `json:"schema"`
	BinlogFile    string    `json:"binlog_file"`
	BinlogPos     uint32    `json:"binlog_pos"`
	Offset        int64     `json:"offset"` // 全量同步偏移量
	ProcessedRows int64     `json:"processed_rows"`
	LastUpdate    time.Time `json:"last_update"`
}

// Manager 位点管理器接口
type Manager interface {
	// Save 保存位点
	Save(ctx context.Context, cp *Checkpoint) error
	// Get 获取位点
	Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error)
	// GetAll 获取任务所有表的位点
	GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error)
	// Delete 删除位点
	Delete(ctx context.Context, taskID string) error
	// GetPosition 获取binlog位置
	GetPosition(ctx context.Context, taskID string) (mysql.Position, error)
	// SavePosition 保存binlog位置
	SavePosition(ctx context.Context, taskID string, pos mysql.Position) error
}

// RedisCheckpointManager Redis位点管理器
type RedisCheckpointManager struct {
	client *redis.Client
	prefix string
}

// NewRedisCheckpointManager 创建Redis位点管理器
func NewRedisCheckpointManager(client *redis.Client, prefix string) *RedisCheckpointManager {
	if prefix == "" {
		prefix = "dts:checkpoint"
	}
	return &RedisCheckpointManager{
		client: client,
		prefix: prefix,
	}
}

// getKey 生成Redis键
func (m *RedisCheckpointManager) getKey(taskID, tableName string) string {
	return fmt.Sprintf("%s:%s:%s", m.prefix, taskID, tableName)
}

// getTaskKey 生成任务键
func (m *RedisCheckpointManager) getTaskKey(taskID string) string {
	return fmt.Sprintf("%s:%s", m.prefix, taskID)
}

// getPositionKey 生成位置键
func (m *RedisCheckpointManager) getPositionKey(taskID string) string {
	return fmt.Sprintf("%s:%s:position", m.prefix, taskID)
}

// Save 保存位点
func (m *RedisCheckpointManager) Save(ctx context.Context, cp *Checkpoint) error {
	cp.LastUpdate = time.Now()
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	key := m.getKey(cp.TaskID, cp.TableName)
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return nil
}

// Get 获取位点
func (m *RedisCheckpointManager) Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error) {
	key := m.getKey(taskID, tableName)
	data, err := m.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // 没有找到位点
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal([]byte(data), &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

// GetAll 获取任务所有表的位点
func (m *RedisCheckpointManager) GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error) {
	pattern := m.getKey(taskID, "*")
	keys, err := m.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	checkpoints := make(map[string]*Checkpoint)
	for _, key := range keys {
		data, err := m.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var cp Checkpoint
		if err := json.Unmarshal([]byte(data), &cp); err != nil {
			continue
		}
		checkpoints[cp.TableName] = &cp
	}

	return checkpoints, nil
}

// Delete 删除位点
func (m *RedisCheckpointManager) Delete(ctx context.Context, taskID string) error {
	pattern := m.getKey(taskID, "*")
	keys, err := m.client.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to list keys: %w", err)
	}

	if len(keys) > 0 {
		if err := m.client.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("failed to delete checkpoints: %w", err)
		}
	}

	// 删除位置键
	posKey := m.getPositionKey(taskID)
	m.client.Del(ctx, posKey)

	return nil
}

// GetPosition 获取binlog位置
func (m *RedisCheckpointManager) GetPosition(ctx context.Context, taskID string) (mysql.Position, error) {
	key := m.getPositionKey(taskID)
	data, err := m.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return mysql.Position{}, nil
	}
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get position: %w", err)
	}

	var pos struct {
		Name string `json:"name"`
		Pos  uint32 `json:"pos"`
	}
	if err := json.Unmarshal([]byte(data), &pos); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to unmarshal position: %w", err)
	}

	return mysql.Position{
		Name: pos.Name,
		Pos:  pos.Pos,
	}, nil
}

// SavePosition 保存binlog位置
func (m *RedisCheckpointManager) SavePosition(ctx context.Context, taskID string, pos mysql.Position) error {
	data, err := json.Marshal(struct {
		Name string `json:"name"`
		Pos  uint32 `json:"pos"`
	}{
		Name: pos.Name,
		Pos:  pos.Pos,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	key := m.getPositionKey(taskID)
	if err := m.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save position: %w", err)
	}

	return nil
}

// MemoryCheckpointManager 内存位点管理器（用于测试或无Redis场景）
type MemoryCheckpointManager struct {
	checkpoints map[string]*Checkpoint
	positions   map[string]mysql.Position
}

// NewMemoryCheckpointManager 创建内存位点管理器
func NewMemoryCheckpointManager() *MemoryCheckpointManager {
	return &MemoryCheckpointManager{
		checkpoints: make(map[string]*Checkpoint),
		positions:   make(map[string]mysql.Position),
	}
}

// getKey 生成键
func (m *MemoryCheckpointManager) getKey(taskID, tableName string) string {
	return fmt.Sprintf("%s:%s", taskID, tableName)
}

// Save 保存位点
func (m *MemoryCheckpointManager) Save(ctx context.Context, cp *Checkpoint) error {
	cp.LastUpdate = time.Now()
	key := m.getKey(cp.TaskID, cp.TableName)
	m.checkpoints[key] = cp
	return nil
}

// Get 获取位点
func (m *MemoryCheckpointManager) Get(ctx context.Context, taskID, tableName string) (*Checkpoint, error) {
	key := m.getKey(taskID, tableName)
	return m.checkpoints[key], nil
}

// GetAll 获取任务所有表的位点
func (m *MemoryCheckpointManager) GetAll(ctx context.Context, taskID string) (map[string]*Checkpoint, error) {
	result := make(map[string]*Checkpoint)
	for k, v := range m.checkpoints {
		if len(k) > len(taskID)+1 && k[:len(taskID)] == taskID {
			result[v.TableName] = v
		}
	}
	return result, nil
}

// Delete 删除位点
func (m *MemoryCheckpointManager) Delete(ctx context.Context, taskID string) error {
	for k := range m.checkpoints {
		if len(k) > len(taskID)+1 && k[:len(taskID)] == taskID {
			delete(m.checkpoints, k)
		}
	}
	delete(m.positions, taskID)
	return nil
}

// GetPosition 获取binlog位置
func (m *MemoryCheckpointManager) GetPosition(ctx context.Context, taskID string) (mysql.Position, error) {
	return m.positions[taskID], nil
}

// SavePosition 保存binlog位置
func (m *MemoryCheckpointManager) SavePosition(ctx context.Context, taskID string, pos mysql.Position) error {
	m.positions[taskID] = pos
	return nil
}
