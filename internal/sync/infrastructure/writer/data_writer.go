package writer

import (
	"context"
	"database/sql"
	"log"
	"mysql-to-async/internal/audit"
	"mysql-to-async/internal/metadata/domain/entity"
	"sync"
	"time"
)

// DataWriter 数据写入器接口
type DataWriter interface {
	// WriteBatch 批量写入数据
	WriteBatch(ctx context.Context, rows []map[string]interface{}) error
	// UpdateWithBeforeImage 使用 before image 进行更新（无主键表）
	UpdateWithBeforeImage(ctx context.Context, row, beforeImage map[string]interface{}) error
	// Update 更新数据
	Update(ctx context.Context, row map[string]interface{}) error
	// Delete 删除数据
	Delete(ctx context.Context, row map[string]interface{}) error
}

// SQLExecutor SQL 执行器接口，*sql.DB 和 *sql.Conn 均满足
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// BatchWriter 批量写入器
type BatchWriter struct {
	db          SQLExecutor
	sqlBuilder  *SQLBuilder
	batchSize   int
	timeout     time.Duration
	auditLogger *audit.AuditLogger
	taskID      string
	schema      string
	tableName   string
}

// NewBatchWriter 创建批量写入器
func NewBatchWriter(db *sql.DB, identity *entity.TableIdentity, batchSize int) *BatchWriter {
	return &BatchWriter{
		db:         db,
		sqlBuilder: NewSQLBuilder(identity),
		batchSize:  batchSize,
		timeout:    300 * time.Second,
	}
}

// NewBatchWriterWithSchema 创建带schema的批量写入器（推荐，确保数据写入正确的schema）
func NewBatchWriterWithSchema(db *sql.DB, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter {
	return &BatchWriter{
		db:         db,
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema),
		batchSize:  batchSize,
		timeout:    300 * time.Second,
	}
}

// NewBatchWriterWithTx 创建使用事务的批量写入器（*sql.Tx 满足 SQLExecutor 接口）
func NewBatchWriterWithTx(tx *sql.Tx, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter {
	return &BatchWriter{
		db:         tx,
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema),
		batchSize:  batchSize,
		timeout:    300 * time.Second,
	}
}

// NewBatchWriterWithConn 创建使用固定连接的批量写入器
// 使用固定连接可确保 SET SESSION 变量（如 FOREIGN_KEY_CHECKS=0）在整个写入过程中生效
func NewBatchWriterWithConn(conn *sql.Conn, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter {
	return &BatchWriter{
		db:         conn,
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema),
		batchSize:  batchSize,
		timeout:    300 * time.Second,
	}
}

// SetAuditLogger 设置审计日志器
func (w *BatchWriter) SetAuditLogger(logger *audit.AuditLogger, taskID, schema, tableName string) {
	w.auditLogger = logger
	w.taskID = taskID
	w.schema = schema
	w.tableName = tableName
}

// WriteBatch 批量写入数据
func (w *BatchWriter) WriteBatch(ctx context.Context, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	query, args := w.sqlBuilder.BuildBatchInsert(rows)
	if query == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	_, err := w.db.ExecContext(ctx, query, args...)

	// 记录审计日志
	if w.auditLogger != nil {
		success := err == nil
		errorMsg := ""
		if err != nil {
			errorMsg = err.Error()
		}
		w.auditLogger.LogDataWrite(w.taskID, w.schema, w.tableName, int64(len(rows)), success, errorMsg)
	}

	return err
}

// Update 更新数据
func (w *BatchWriter) Update(ctx context.Context, row map[string]interface{}) error {
	query, args := w.sqlBuilder.BuildUpdate(row)

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		// 记录失败审计日志
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, false, err.Error(), row)
		}
		return err
	}

	// 检查是否匹配到行（无主键表安全检查）
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// 记录数据空漂移异常（审计日志）
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)", row)
		}
		log.Printf("[Task %s] Warning: UPDATE matched 0 rows for table %s, possible data drift", w.taskID, w.tableName)
	} else {
		// 记录成功审计日志
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "", nil)
		}
	}

	return nil
}

// UpdateWithBeforeImage 使用 before image 进行更新（针对无主键表）
func (w *BatchWriter) UpdateWithBeforeImage(ctx context.Context, row, beforeImage map[string]interface{}) error {
	query, args := w.sqlBuilder.BuildUpdateWithBeforeImage(row, beforeImage)

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, false, err.Error(), row)
		}
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)", row)
		}
		log.Printf("[Task %s] Warning: UPDATE (with before image) matched 0 rows for table %s, possible data drift", w.taskID, w.tableName)
	} else {
		if w.auditLogger != nil {
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "", nil)
		}
	}

	return nil
}

// Delete 删除数据
func (w *BatchWriter) Delete(ctx context.Context, row map[string]interface{}) error {
	query, args := w.sqlBuilder.BuildDelete(row)

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		// 记录失败审计日志
		if w.auditLogger != nil {
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, false, err.Error())
		}
		return err
	}

	// 检查是否匹配到行
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// 记录数据空漂移异常
		if w.auditLogger != nil {
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)")
		}
		log.Printf("[Task %s] Warning: DELETE matched 0 rows for table %s, possible data drift", w.taskID, w.tableName)
	} else {
		// 记录成功审计日志
		if w.auditLogger != nil {
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, true, "")
		}
	}

	return nil
}

// BufferedWriter 带缓冲的写入器
type BufferedWriter struct {
	writer        *BatchWriter
	buffer        []map[string]interface{}
	mu            sync.Mutex
	batchSize     int
	flushInterval time.Duration
	stopCh        chan struct{}
}

// NewBufferedWriter 创建缓冲写入器
func NewBufferedWriter(db *sql.DB, identity *entity.TableIdentity, batchSize int, flushInterval time.Duration) *BufferedWriter {
	w := &BufferedWriter{
		writer:        NewBatchWriter(db, identity, batchSize),
		buffer:        make([]map[string]interface{}, 0, batchSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	// 启动定时刷新协程
	go w.flushLoop()

	return w
}

// NewBufferedWriterWithSchema 创建带schema的缓冲写入器（推荐，确保数据写入正确的schema）
func NewBufferedWriterWithSchema(db *sql.DB, identity *entity.TableIdentity, batchSize int, flushInterval time.Duration, schema string) *BufferedWriter {
	w := &BufferedWriter{
		writer:        NewBatchWriterWithSchema(db, identity, batchSize, schema),
		buffer:        make([]map[string]interface{}, 0, batchSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	// 启动定时刷新协程
	go w.flushLoop()

	return w
}

// Write 写入数据到缓冲区
func (w *BufferedWriter) Write(row map[string]interface{}) error {
	w.mu.Lock()
	w.buffer = append(w.buffer, row)
	shouldFlush := len(w.buffer) >= w.batchSize
	w.mu.Unlock()

	if shouldFlush {
		return w.Flush()
	}
	return nil
}

// Flush 刷新缓冲区
func (w *BufferedWriter) Flush() error {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return nil
	}
	rows := w.buffer
	w.buffer = make([]map[string]interface{}, 0, w.batchSize)
	w.mu.Unlock()

	return w.writer.WriteBatch(context.Background(), rows)
}

// flushLoop 定时刷新循环
func (w *BufferedWriter) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.stopCh:
			w.Flush()
			return
		}
	}
}

// Close 关闭写入器
func (w *BufferedWriter) Close() error {
	close(w.stopCh)
	return w.Flush()
}

// GetSQLBuilder 获取SQL构建器（用于增量同步）
func (w *BatchWriter) GetSQLBuilder() *SQLBuilder {
	return w.sqlBuilder
}
