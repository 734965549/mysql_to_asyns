package readonly

import (
	"context"
	"database/sql"
	"fmt"
	"mysql-to-async/pkg/logger"
	"sync"
)

// ReadOnlyManager 管理目标数据库的只读状态
type ReadOnlyManager struct {
	targetDB      *sql.DB
	originalState *readOnlyState // 保存原始只读状态
	mu            sync.Mutex
}

// readOnlyState 保存只读状态（同时跟踪 read_only 和 super_read_only）
type readOnlyState struct {
	ReadOnly      bool
	SuperReadOnly bool
}

// NewReadOnlyManager 创建只读管理器
func NewReadOnlyManager(targetDB *sql.DB) *ReadOnlyManager {
	return &ReadOnlyManager{
		targetDB: targetDB,
	}
}

// SetReadOnly 同步开始时调用：
//   - 保存 read_only / super_read_only 原始状态
//   - 设置 super_read_only=OFF, read_only=ON
//     → 普通用户无法写入（保护目标库），SUPER 用户（root）可以写入同步数据
func (m *ReadOnlyManager) SetReadOnly() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Info("[ReadOnlyManager] 开始设置目标实例为只读模式...")

	originalState, err := m.getReadOnlyState()
	if err != nil {
		return fmt.Errorf("获取当前只读状态失败: %v", err)
	}
	m.originalState = originalState

	logger.Info("[ReadOnlyManager] 当前状态: read_only=%v super_read_only=%v",
		originalState.ReadOnly, originalState.SuperReadOnly)

	// 先关 super_read_only（必须先关，否则 read_only 操作本身也会被拦截）
	if originalState.SuperReadOnly {
		if _, err = m.targetDB.Exec("SET GLOBAL super_read_only = 0"); err != nil {
			return fmt.Errorf("关闭 super_read_only 失败: %v", err)
		}
	}
	// 确保 read_only=ON（阻止非 SUPER 用户写入）
	if _, err = m.targetDB.Exec("SET GLOBAL read_only = 1"); err != nil {
		return fmt.Errorf("设置 read_only 失败: %v", err)
	}

	logger.Info("[ReadOnlyManager] 目标实例: super_read_only=OFF read_only=ON，SUPER 用户可写入同步数据")
	return nil
}

// RestoreReadOnly 同步结束时调用：恢复 read_only / super_read_only 到原始状态
func (m *ReadOnlyManager) RestoreReadOnly() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.originalState == nil {
		logger.Info("[ReadOnlyManager] 没有保存的只读状态，跳过恢复")
		return nil
	}

	logger.Info("[ReadOnlyManager] 开始恢复目标实例的读写状态...")

	var err error
	// 恢复顺序：先恢复 read_only，再恢复 super_read_only
	roVal := 0
	if m.originalState.ReadOnly {
		roVal = 1
	}
	if _, err = m.targetDB.Exec("SET GLOBAL read_only = ?", roVal); err != nil {
		return fmt.Errorf("恢复 read_only 失败: %v", err)
	}

	sroVal := 0
	if m.originalState.SuperReadOnly {
		sroVal = 1
	}
	if _, err = m.targetDB.Exec("SET GLOBAL super_read_only = ?", sroVal); err != nil {
		return fmt.Errorf("恢复 super_read_only 失败: %v", err)
	}

	logger.Info("[ReadOnlyManager] 已恢复: read_only=%v super_read_only=%v",
		m.originalState.ReadOnly, m.originalState.SuperReadOnly)

	m.originalState = nil
	return nil
}

// WithWriteAccess 临时关闭 read_only 和 super_read_only，执行 DDL，完成后恢复。
// 在 SetReadOnly() 之后调用时，执行完恢复为 super_read_only=OFF, read_only=ON。
func (m *ReadOnlyManager) WithWriteAccess(fn func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.getReadOnlyState()
	if err != nil {
		logger.Warn("[ReadOnlyManager] 无法查询只读状态，直接执行写操作: %v", err)
		return fn()
	}

	needRestore := state.ReadOnly || state.SuperReadOnly
	if !needRestore {
		return fn()
	}

	// 先关 super_read_only，再关 read_only
	if state.SuperReadOnly {
		if _, err = m.targetDB.Exec("SET GLOBAL super_read_only = 0"); err != nil {
			return fmt.Errorf("临时关闭 super_read_only 失败: %v", err)
		}
	}
	if state.ReadOnly {
		if _, err = m.targetDB.Exec("SET GLOBAL read_only = 0"); err != nil {
			return fmt.Errorf("临时关闭 read_only 失败: %v", err)
		}
	}
	logger.Info("[ReadOnlyManager] 已临时开放写权限以执行 DDL")

	defer func() {
		// 恢复：先 read_only=ON，再 super_read_only（如果原本就没有则保持 OFF）
		if state.ReadOnly {
			if _, e := m.targetDB.Exec("SET GLOBAL read_only = 1"); e != nil {
				logger.Warn("[ReadOnlyManager] 恢复 read_only 失败: %v", e)
			}
		}
		// DDL 后 super_read_only 保持 OFF（由 SetReadOnly 统一管理），不在此恢复
		logger.Info("[ReadOnlyManager] DDL 完成，read_only 已恢复")
	}()

	return fn()
}

// getReadOnlyState 获取当前 read_only 和 super_read_only 状态（不加锁，供内部使用）
func (m *ReadOnlyManager) getReadOnlyState() (*readOnlyState, error) {
	conn, err := m.targetDB.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %v", err)
	}
	defer conn.Close()

	state := &readOnlyState{}
	err = conn.QueryRowContext(context.Background(),
		"SELECT @@read_only, @@super_read_only",
	).Scan(&state.ReadOnly, &state.SuperReadOnly)
	if err != nil {
		// 部分 MySQL 版本不支持 super_read_only，降级只查 read_only
		err2 := conn.QueryRowContext(context.Background(), "SELECT @@read_only").Scan(&state.ReadOnly)
		if err2 != nil {
			return nil, fmt.Errorf("查询 read_only 失败: %v", err2)
		}
	}
	return state, nil
}

// IsReadOnly 检查当前是否处于只读模式（read_only 或 super_read_only 任一为 ON）
func (m *ReadOnlyManager) IsReadOnly() (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.getReadOnlyState()
	if err != nil {
		return false, err
	}
	return state.ReadOnly || state.SuperReadOnly, nil
}

// setGlobalReadOnly 保留供测试兼容（仅设置 read_only）
func (m *ReadOnlyManager) setGlobalReadOnly(readOnly bool) error {
	val := 0
	if readOnly {
		val = 1
	}
	_, err := m.targetDB.Exec("SET GLOBAL read_only = ?", val)
	return err
}

// restoreGlobalReadOnly 保留供测试兼容
func (m *ReadOnlyManager) restoreGlobalReadOnly(state *readOnlyState) error {
	val := 0
	if state.ReadOnly {
		val = 1
	}
	_, err := m.targetDB.Exec("SET GLOBAL read_only = ?", val)
	return err
}
