package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

func TestCheckpoint_Struct(t *testing.T) {
	cp := Checkpoint{
		TaskID:        "task-123",
		TableName:     "users",
		Schema:        "test_db",
		BinlogFile:    "binlog.000001",
		BinlogPos:     12345,
		Offset:        1000,
		ProcessedRows: 5000,
		LastUpdate:    time.Now(),
	}

	assert.Equal(t, "task-123", cp.TaskID)
	assert.Equal(t, "users", cp.TableName)
	assert.Equal(t, "test_db", cp.Schema)
	assert.Equal(t, "binlog.000001", cp.BinlogFile)
	assert.Equal(t, uint32(12345), cp.BinlogPos)
	assert.Equal(t, int64(1000), cp.Offset)
	assert.Equal(t, int64(5000), cp.ProcessedRows)
}

func TestMemoryCheckpointManager_Save(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	cp := &Checkpoint{
		TaskID:    "task_1",
		TableName: "users",
		Schema:    "test_db",
	}

	err := manager.Save(ctx, cp)
	assert.NoError(t, err)
}

func TestMemoryCheckpointManager_Get(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 保存位点
	cp := &Checkpoint{
		TaskID:     "task_1",
		TableName:  "users",
		Schema:     "test_db",
		BinlogFile: "binlog.000001",
		BinlogPos:  12345,
	}
	manager.Save(ctx, cp)

	// 获取位点
	loaded, err := manager.Get(ctx, "task_1", "users")
	assert.NoError(t, err)
	assert.NotNil(t, loaded)
	assert.Equal(t, "task_1", loaded.TaskID)
	assert.Equal(t, "users", loaded.TableName)
	assert.Equal(t, "binlog.000001", loaded.BinlogFile)
	assert.Equal(t, uint32(12345), loaded.BinlogPos)
}

func TestMemoryCheckpointManager_Get_NonExistent(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 获取不存在的位点应该返回nil
	loaded, err := manager.Get(ctx, "non_existent", "table")
	assert.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestMemoryCheckpointManager_Delete(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 保存位点
	cp := &Checkpoint{
		TaskID:    "task_1",
		TableName: "users",
		Schema:    "test_db",
	}
	manager.Save(ctx, cp)

	// 删除位点
	err := manager.Delete(ctx, "task_1")
	assert.NoError(t, err)

	// 验证已删除
	loaded, _ := manager.Get(ctx, "task_1", "users")
	assert.Nil(t, loaded)
}

func TestMemoryCheckpointManager_GetAll(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 保存多个表的位点
	for i := 1; i <= 3; i++ {
		cp := &Checkpoint{
			TaskID:    "task_1",
			TableName: "table_" + string(rune('0'+i)),
			Schema:    "test_db",
			Offset:    int64(i * 1000),
		}
		manager.Save(ctx, cp)
	}

	// 获取所有位点
	all, err := manager.GetAll(ctx, "task_1")
	assert.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestMemoryCheckpointManager_SavePosition(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 保存位置
	pos := mysql.Position{
		Name: "binlog.000001",
		Pos:  12345,
	}
	manager.SavePosition(ctx, "task_1", pos)

	// 获取位置
	loaded, err := manager.GetPosition(ctx, "task_1")
	assert.NoError(t, err)
	assert.Equal(t, "binlog.000001", loaded.Name)
	assert.Equal(t, uint32(12345), loaded.Pos)
}

func TestMemoryCheckpointManager_GetPosition_NonExistent(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 获取不存在的位置应该返回空位置
	loaded, err := manager.GetPosition(ctx, "non_existent")
	assert.NoError(t, err)
	assert.Equal(t, "", loaded.Name)
	assert.Equal(t, uint32(0), loaded.Pos)
}

func TestMemoryCheckpointManager_MultipleTasks(t *testing.T) {
	manager := NewMemoryCheckpointManager()
	ctx := context.Background()

	// 保存多个任务的位点
	for i := 1; i <= 5; i++ {
		cp := &Checkpoint{
			TaskID:    "task_" + string(rune('0'+i)),
			TableName: "users",
			Schema:    "test_db",
			Offset:    int64(i * 1000),
		}
		manager.Save(ctx, cp)
	}

	// 验证每个任务的位点都正确保存
	for i := 1; i <= 5; i++ {
		taskID := "task_" + string(rune('0'+i))
		loaded, _ := manager.Get(ctx, taskID, "users")
		assert.NotNil(t, loaded)
		assert.Equal(t, int64(i*1000), loaded.Offset)
	}
}

// RedisCheckpointManager 测试（使用mock）
func TestRedisCheckpointManager_New(t *testing.T) {
	// 测试默认前缀
	manager := NewRedisCheckpointManager(nil, "")
	assert.Equal(t, "dts:checkpoint", manager.prefix)

	// 测试自定义前缀
	manager2 := NewRedisCheckpointManager(nil, "custom:prefix")
	assert.Equal(t, "custom:prefix", manager2.prefix)
}

func TestRedisCheckpointManager_getKey(t *testing.T) {
	manager := NewRedisCheckpointManager(nil, "test")
	key := manager.getKey("task_1", "users")
	assert.Equal(t, "test:task_1:users", key)
}

func TestRedisCheckpointManager_getTaskKey(t *testing.T) {
	manager := NewRedisCheckpointManager(nil, "test")
	key := manager.getTaskKey("task_1")
	assert.Equal(t, "test:task_1", key)
}

func TestRedisCheckpointManager_getPositionKey(t *testing.T) {
	manager := NewRedisCheckpointManager(nil, "test")
	key := manager.getPositionKey("task_1")
	assert.Equal(t, "test:task_1:position", key)
}
