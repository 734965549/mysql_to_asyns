package readonly

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestNewReadOnlyManager(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.targetDB)
}

func TestReadOnlyManager_SetReadOnly_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))
	// 模拟设置 read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	err = manager.SetReadOnly()
	assert.NoError(t, err)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_SetReadOnly_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询失败
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnError(errors.New("query error"))

	err = manager.SetReadOnly()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "获取当前只读状态失败")

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_SetReadOnly_SetError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询成功
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))
	// 模拟设置失败
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnError(errors.New("set error"))

	err = manager.SetReadOnly()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "设置 read_only 失败")

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_RestoreReadOnly_NoState(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)
	// 没有保存的状态，应该直接返回nil
	err = manager.RestoreReadOnly()
	assert.NoError(t, err)
}

func TestReadOnlyManager_RestoreReadOnly_WithState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))
	// 模拟设置 read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	// 先设置只读
	err = manager.SetReadOnly()
	assert.NoError(t, err)

	// 模拟恢复 read_only 与 super_read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL super_read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	// 恢复只读状态
	err = manager.RestoreReadOnly()
	assert.NoError(t, err)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_RestoreReadOnly_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))
	// 模拟设置 read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	// 先设置只读
	err = manager.SetReadOnly()
	assert.NoError(t, err)

	// 模拟恢复失败
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnError(errors.New("restore error"))

	// 恢复只读状态
	err = manager.RestoreReadOnly()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "恢复 read_only 失败")

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_IsReadOnly_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(1, 0))

	readOnly, err := manager.IsReadOnly()
	assert.NoError(t, err)
	assert.True(t, readOnly)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_IsReadOnly_False(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))

	readOnly, err := manager.IsReadOnly()
	assert.NoError(t, err)
	assert.False(t, readOnly)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_IsReadOnly_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询失败
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnError(errors.New("query error"))

	readOnly, err := manager.IsReadOnly()
	assert.Error(t, err)
	assert.False(t, readOnly)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_GetReadOnlyState_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(1, 0))

	// 使用反射调用私有方法进行测试
	// 这里我们通过公共方法间接测试
	readOnly, err := manager.IsReadOnly()
	assert.NoError(t, err)
	assert.True(t, readOnly)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_GetReadOnlyState_ConnectionError(t *testing.T) {
	// 创建一个已关闭的数据库连接
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	db.Close()

	manager := NewReadOnlyManager(db)

	// 尝试查询只读状态
	_, err = manager.IsReadOnly()
	assert.Error(t, err)
}

func TestReadOnlyManager_SetGlobalReadOnly_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 先设置只读状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(0, 0))
	// 模拟设置 read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	err = manager.SetReadOnly()
	assert.NoError(t, err)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadOnlyManager_RestoreGlobalReadOnly_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock db: %v", err)
	}
	defer db.Close()

	manager := NewReadOnlyManager(db)

	// 模拟查询 read_only/super_read_only 状态
	mock.ExpectQuery("SELECT @@read_only, @@super_read_only").WillReturnRows(sqlmock.NewRows([]string{"@@read_only", "@@super_read_only"}).AddRow(1, 0))
	// 模拟设置 read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	// 先设置只读
	err = manager.SetReadOnly()
	assert.NoError(t, err)

	// 模拟恢复 read_only 与 super_read_only
	mock.ExpectExec("SET GLOBAL read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL super_read_only = ?").WillReturnResult(sqlmock.NewResult(0, 0))

	// 恢复只读状态
	err = manager.RestoreReadOnly()
	assert.NoError(t, err)

	// 验证所有期望都被满足
	assert.NoError(t, mock.ExpectationsWereMet())
}
