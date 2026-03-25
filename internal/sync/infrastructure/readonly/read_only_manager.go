package readonly

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
)

// ReadOnlyManager 管理目标数据库的只读状态
type ReadOnlyManager struct {
	targetDB      *sql.DB
	originalState *readOnlyState // 保存原始只读状态
	mu            sync.RWMutex
}

// readOnlyState 保存只读状态
type readOnlyState struct {
	ReadOnly bool
}

// NewReadOnlyManager 创建只读管理器
func NewReadOnlyManager(targetDB *sql.DB) *ReadOnlyManager {
	return &ReadOnlyManager{
		targetDB: targetDB,
	}
}

// SetReadOnly 设置目标实例为只读模式
// 使用 MySQL 的 read_only 全局变量
// 注意：read_only 只限制普通用户，超级用户仍可写入
func (m *ReadOnlyManager) SetReadOnly() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Println("[ReadOnlyManager] 开始设置目标实例为只读模式...")

	// 1. 先保存当前的只读状态
	originalState, err := m.getReadOnlyState()
	if err != nil {
		return fmt.Errorf("获取当前只读状态失败: %v", err)
	}
	m.originalState = originalState

	log.Printf("[ReadOnlyManager] 当前状态: read_only=%v\n", originalState.ReadOnly)

	// 2. 设置 read_only = ON
	// read_only 只限制普通用户，超级用户仍可写入
	err = m.setGlobalReadOnly(true)
	if err != nil {
		return fmt.Errorf("设置只读模式失败: %v", err)
	}

	log.Println("[ReadOnlyManager] 目标实例已设置为只读模式 (read_only=ON)，超级用户仍可写入")
	return nil
}

// RestoreReadOnly 恢复原始的只读状态
func (m *ReadOnlyManager) RestoreReadOnly() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.originalState == nil {
		log.Println("[ReadOnlyManager] 没有保存的只读状态，跳过恢复")
		return nil
	}

	log.Println("[ReadOnlyManager] 开始恢复目标实例的读写状态...")

	// 恢复到原始状态
	err := m.restoreGlobalReadOnly(m.originalState)
	if err != nil {
		return fmt.Errorf("恢复读写状态失败: %v", err)
	}

	log.Printf("[ReadOnlyManager] 目标实例已恢复读写状态: read_only=%v\n",
		m.originalState.ReadOnly)

	// 清空保存的状态
	m.originalState = nil
	return nil
}

// getReadOnlyState 获取当前的只读状态
func (m *ReadOnlyManager) getReadOnlyState() (*readOnlyState, error) {
	state := &readOnlyState{}

	// 使用独立的连接执行查询，避免 "commands out of sync" 错误
	// 获取连接
	conn, err := m.targetDB.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %v", err)
	}
	defer conn.Close()

	// 获取 read_only 状态
	err = conn.QueryRowContext(context.Background(), "SELECT @@read_only").Scan(&state.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("查询 read_only 失败: %v", err)
	}

	return state, nil
}

// setGlobalReadOnly 设置全局只读模式
func (m *ReadOnlyManager) setGlobalReadOnly(readOnly bool) error {
	var value int
	if readOnly {
		value = 1
	} else {
		value = 0
	}

	// 只使用 read_only，不使用 super_read_only
	// 这样超级用户仍可写入数据
	_, err := m.targetDB.Exec("SET GLOBAL read_only = ?", value)
	if err != nil {
		return fmt.Errorf("设置只读模式失败: %v", err)
	}

	return nil
}

// restoreGlobalReadOnly 恢复到指定的只读状态
func (m *ReadOnlyManager) restoreGlobalReadOnly(state *readOnlyState) error {
	var readOnlyValue int
	if state.ReadOnly {
		readOnlyValue = 1
	} else {
		readOnlyValue = 0
	}

	_, err := m.targetDB.Exec("SET GLOBAL read_only = ?", readOnlyValue)
	if err != nil {
		return fmt.Errorf("恢复只读状态失败: %v", err)
	}

	return nil
}

// IsReadOnly 检查当前是否处于只读模式
func (m *ReadOnlyManager) IsReadOnly() (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, err := m.getReadOnlyState()
	if err != nil {
		return false, err
	}

	return state.ReadOnly, nil
}