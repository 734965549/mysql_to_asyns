package writer // 声明当前文件属于writer包，用于数据写入

import ( // 导入外部包和标准库
	"context"                                       // 导入context包，用于上下文管理
	"database/sql"                                  // 导入database/sql包，用于数据库操作
	"fmt"                                           // 导入fmt包，用于格式化输入输出
	"mysql-to-sync/internal/audit"                  // 导入审计包
	"mysql-to-sync/internal/metadata/domain/entity" // 导入实体包
	"mysql-to-sync/internal/metrics"                // 导入指标包，用于零行匹配等漂移指标
	"mysql-to-sync/pkg/logger"                      // 导入log包，用于日志输出
	"sync"                                          // 导入sync包，用于并发控制
	"sync/atomic"                                   // 导入atomic，用于暂停后台 auto-flush
	"time"                                          // 导入time包，用于时间处理
)

// mysqlMaxPreparedPlaceholders MySQL 单条预处理语句占位符上限（留余量）
const mysqlMaxPreparedPlaceholders = 62000 // MySQL预处理语句占位符上限

// DataWriter 数据写入器接口
// DataWriter is the target write contract used by full and incremental sync.
//
// Full sync enables plain INSERT at the service layer and requires an empty
// target table. Incremental sync enables upsert on buffered writers for PK/UK
// tables and uses before-image matching for no-primary-key UPDATE events.
type DataWriter interface { // 定义数据写入器接口
	// WriteBatch 批量写入数据方法
	WriteBatch(ctx context.Context, rows []map[string]interface{}) error // 批量写入数据
	// UpdateWithBeforeImage 使用 before image 进行更新（无主键表）方法
	UpdateWithBeforeImage(ctx context.Context, row, beforeImage map[string]interface{}) error // 使用before image更新
	// Update 更新数据方法
	Update(ctx context.Context, row map[string]interface{}) error // 更新数据
	// Delete 删除数据方法
	Delete(ctx context.Context, row map[string]interface{}) error // 删除数据
}

// SQLExecutor SQL 执行器接口，*sql.DB 和 *sql.Conn 均满足
type SQLExecutor interface { // 定义SQL执行器接口
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) // 执行SQL语句
}

// BatchWriter 批量写入器
// BatchWriter executes deterministic SQL generated from TableIdentity.
//
// useUpsert is deliberately opt-in: full sync enables plain INSERT at the
// service layer, while incremental sync must enable upsert so repeated PK/UK
// INSERT events converge to the source row.
type BatchWriter struct { // 定义批量写入器结构体
	db          SQLExecutor        // SQL执行器
	sqlBuilder  *SQLBuilder        // SQL构建器
	batchSize   int                // 批处理大小
	timeout     time.Duration      // 超时时间
	auditLogger *audit.AuditLogger // 审计日志器
	taskID      string             // 任务ID
	schema      string             // 数据库schema
	tableName   string             // 表名
	// useUpsert 控制 WriteBatch 是否使用 "INSERT ... ON DUPLICATE KEY UPDATE" 形式。
	// 增量同步路径必须设为 true，否则同一行的重复 INSERT 事件不会更新已有行
	//（破坏修复 10/11 的幂等承诺）。全量同步路径在服务层启用 usePlainInsert。
	useUpsert bool

	// usePlainInsert 控制 WriteBatch 是否使用普通 INSERT（无 IGNORE，无 ON DUPLICATE KEY UPDATE）。
	// 全量同步路径统一启用；目标端必须由用户保证为空，或通过 enable_drop_table_before_ddl 重建为空。
	// 优先级高于 useUpsert。
	usePlainInsert bool
}

// EnableUpsert 切换 WriteBatch 进入 upsert 语义。增量同步路径必须调用此方法，否则
// 同一 PK/UK 的后续 INSERT 事件会被 IGNORE 而不更新行内容。
func (w *BatchWriter) EnableUpsert() {
	w.useUpsert = true
}

// EnablePlainInsert 切换 WriteBatch 进入普通 INSERT 语义（无 IGNORE，无 ON DUPLICATE KEY UPDATE）。
//
// 全量同步路径统一调用；目标端必须由用户保证为空，或通过 enable_drop_table_before_ddl 重建为空。
// 与 useUpsert 互斥：usePlainInsert 优先级最高，其次是 useUpsert，默认 INSERT IGNORE。
func (w *BatchWriter) EnablePlainInsert() {
	w.usePlainInsert = true
}

// NewBatchWriter 创建批量写入器函数
func NewBatchWriter(db *sql.DB, identity *entity.TableIdentity, batchSize int) *BatchWriter { // 创建批量写入器实例
	return &BatchWriter{ // 返回写入器实例
		db:         db,                      // 设置数据库连接
		sqlBuilder: NewSQLBuilder(identity), // 创建SQL构建器
		batchSize:  batchSize,               // 设置批处理大小
		timeout:    300 * time.Second,       // 设置超时时间
	}
}

// NewBatchWriterWithSchema 创建带schema的批量写入器函数（推荐，确保数据写入正确的schema）
func NewBatchWriterWithSchema(db *sql.DB, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter { // 创建带schema的批量写入器实例
	return &BatchWriter{ // 返回写入器实例
		db:         db,                                        // 设置数据库连接
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema), // 创建带schema的SQL构建器
		batchSize:  batchSize,                                 // 设置批处理大小
		timeout:    300 * time.Second,                         // 设置超时时间
	}
}

// NewBatchWriterWithTx 创建使用事务的批量写入器函数（*sql.Tx 满足 SQLExecutor 接口）
func NewBatchWriterWithTx(tx *sql.Tx, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter { // 创建使用事务的批量写入器实例
	return &BatchWriter{ // 返回写入器实例
		db:         tx,                                        // 设置事务
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema), // 创建带schema的SQL构建器
		batchSize:  batchSize,                                 // 设置批处理大小
		timeout:    300 * time.Second,                         // 设置超时时间
	}
}

// SetExecutor 切换底层 SQL 执行器（例如 autocommit 连接 ↔ 活动事务）。
func (w *BatchWriter) SetExecutor(db SQLExecutor) {
	w.db = db
}

// NewBatchWriterWithConn 创建使用固定连接的批量写入器函数
// 使用固定连接可确保 SET SESSION 变量（如 FOREIGN_KEY_CHECKS=0）在整个写入过程中生效
func NewBatchWriterWithConn(conn *sql.Conn, identity *entity.TableIdentity, batchSize int, schema string) *BatchWriter { // 创建使用固定连接的批量写入器实例
	return &BatchWriter{ // 返回写入器实例
		db:         conn,                                      // 设置固定连接
		sqlBuilder: NewSQLBuilderWithSchema(identity, schema), // 创建带schema的SQL构建器
		batchSize:  batchSize,                                 // 设置批处理大小
		timeout:    300 * time.Second,                         // 设置超时时间
	}
}

// SetAuditLogger 设置审计日志器方法
func (w *BatchWriter) SetAuditLogger(logger *audit.AuditLogger, taskID, schema, tableName string) { // 设置审计日志器
	w.auditLogger = logger  // 设置审计日志器
	w.taskID = taskID       // 设置任务ID
	w.schema = schema       // 设置schema
	w.tableName = tableName // 设置表名
}

// WriteBatch 批量写入数据方法（列多时会自动拆成多条 INSERT，避免占位符超限）
func (w *BatchWriter) WriteBatch(ctx context.Context, rows []map[string]interface{}) error { // 批量写入数据
	if len(rows) == 0 { // 如果没有行数据
		return nil // 返回成功
	}

	nCols := w.sqlBuilder.identity.WritableColumnCount() // 获取可写入列数
	if nCols == 0 {                             // 如果没有列
		return fmt.Errorf("table %s has no columns in identity", w.sqlBuilder.identity.TableName) // 返回错误
	}
	maxRowsPerStmt := mysqlMaxPreparedPlaceholders / nCols // 计算每条语句最大行数
	if maxRowsPerStmt < 1 {                                // 如果小于1
		maxRowsPerStmt = 1 // 设置为最小值1
	}

	ctx, cancel := context.WithTimeout(ctx, w.timeout) // 设置超时上下文
	defer cancel()                                     // 延迟取消

	var lastErr error                                            // 定义最后错误
	for start := 0; start < len(rows); start += maxRowsPerStmt { // 分批处理
		end := start + maxRowsPerStmt // 计算结束位置
		if end > len(rows) {          // 如果超过总行数
			end = len(rows) // 调整为总行数
		}
		chunk := rows[start:end] // 获取当前批次
		// 修复 10/11：增量路径 useUpsert=true → ON DUPLICATE KEY UPDATE，保证 INSERT 事件可重复执行。
		// usePlainInsert 优先级最高：全量同步统一使用普通 INSERT，目标端必须为空。
		var (
			query string
			args  []interface{}
		)
		if w.usePlainInsert {
			query, args = w.sqlBuilder.BuildBatchInsertPlain(chunk)
		} else if w.useUpsert {
			query, args = w.sqlBuilder.BuildBatchUpsert(chunk)
		} else {
			query, args = w.sqlBuilder.BuildBatchInsert(chunk)
		}
		if query == "" { // 如果查询为空
			continue // 跳过
		}
		_, err := w.db.ExecContext(ctx, query, args...) // 执行插入
		if w.auditLogger != nil {                       // 如果有审计日志器
			success := err == nil // 判断是否成功
			errorMsg := ""        // 初始化错误信息
			if err != nil {       // 如果有错误
				errorMsg = err.Error() // 获取错误信息
			}
			w.auditLogger.LogDataWrite(w.taskID, w.schema, w.tableName, int64(len(chunk)), success, errorMsg) // 记录审计日志
		}
		if err != nil { // 如果有错误
			lastErr = err // 记录错误
			break         // 退出循环
		}
	}
	return lastErr // 返回错误
}

// Update 更新数据方法
func (w *BatchWriter) Update(ctx context.Context, row map[string]interface{}) error { // 更新数据
	query, args := w.sqlBuilder.BuildUpdate(row) // 构建更新语句

	ctx, cancel := context.WithTimeout(ctx, w.timeout) // 设置超时上下文
	defer cancel()                                     // 延迟取消

	result, err := w.db.ExecContext(ctx, query, args...) // 执行更新
	if err != nil {                                      // 如果执行失败
		// 记录失败审计日志
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, false, err.Error(), row) // 记录失败日志
		}
		return err // 返回错误
	}

	// 检查是否匹配到行（无主键表安全检查）
	rowsAffected, _ := result.RowsAffected() // 获取影响的行数
	if rowsAffected == 0 {                   // 如果没有匹配的行
		// 修复 10/14：审计 + 指标 + 结构化日志，便于运维定位漂移
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)", row) // 记录警告
		}
		metrics.GetMetrics().IncrementIncrementalZeroRowUpdate()
		logger.Warn("[ZeroRowMatch][Task %s] UPDATE matched 0 rows for table %s.%s; possible target drift (counter: mysql_sync_incremental_zero_row_update_total)",
			w.taskID, w.schema, w.tableName)
	} else { // 否则
		// 记录成功审计日志
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "", nil) // 记录成功日志
		}
	}

	return nil // 返回成功
}

// UpdateWithBeforeImage 使用 before image 进行更新方法（针对无主键表）
func (w *BatchWriter) UpdateWithBeforeImage(ctx context.Context, row, beforeImage map[string]interface{}) error { // 使用before image更新
	query, args := w.sqlBuilder.BuildUpdateWithBeforeImage(row, beforeImage) // 构建更新语句

	ctx, cancel := context.WithTimeout(ctx, w.timeout) // 设置超时上下文
	defer cancel()                                     // 延迟取消

	result, err := w.db.ExecContext(ctx, query, args...) // 执行更新
	if err != nil {                                      // 如果执行失败
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, false, err.Error(), row) // 记录失败日志
		}
		return err // 返回错误
	}

	rowsAffected, _ := result.RowsAffected() // 获取影响的行数
	if rowsAffected == 0 {                   // 如果没有匹配的行
		// 无主键表 update：风险面更高（全列 WHERE + LIMIT 1），同样累计漂移计数
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)", row) // 记录警告
		}
		metrics.GetMetrics().IncrementIncrementalZeroRowUpdate()
		logger.Warn("[ZeroRowMatch][Task %s] UPDATE (before-image) matched 0 rows for table %s.%s; possible target drift on no-PK table",
			w.taskID, w.schema, w.tableName)
	} else { // 否则
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataUpdate(w.taskID, w.schema, w.tableName, true, "", nil) // 记录成功日志
		}
	}

	return nil // 返回成功
}

// Delete 删除数据方法
func (w *BatchWriter) Delete(ctx context.Context, row map[string]interface{}) error { // 删除数据
	query, args := w.sqlBuilder.BuildDelete(row) // 构建删除语句

	ctx, cancel := context.WithTimeout(ctx, w.timeout) // 设置超时上下文
	defer cancel()                                     // 延迟取消

	result, err := w.db.ExecContext(ctx, query, args...) // 执行删除
	if err != nil {                                      // 如果执行失败
		// 记录失败审计日志
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, false, err.Error()) // 记录失败日志
		}
		return err // 返回错误
	}

	// 检查是否匹配到行
	rowsAffected, _ := result.RowsAffected() // 获取影响的行数
	if rowsAffected == 0 {                   // 如果没有匹配的行
		// 修复 10/14：审计 + 指标 + 结构化日志
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, true, "no rows matched (data drift)") // 记录警告
		}
		metrics.GetMetrics().IncrementIncrementalZeroRowDelete()
		logger.Warn("[ZeroRowMatch][Task %s] DELETE matched 0 rows for table %s.%s; possible target drift (counter: mysql_sync_incremental_zero_row_delete_total)",
			w.taskID, w.schema, w.tableName)
	} else { // 否则
		// 记录成功审计日志
		if w.auditLogger != nil { // 如果有审计日志器
			w.auditLogger.LogDataDelete(w.taskID, w.schema, w.tableName, true, "") // 记录成功日志
		}
	}

	return nil // 返回成功
}

// BufferedWriter 带缓冲的写入器
type BufferedWriter struct { // 定义带缓冲的写入器结构体
	writer        *BatchWriter             // 批量写入器
	buffer        []map[string]interface{} // 缓冲区
	mu            sync.Mutex               // 互斥锁
	batchSize     int                      // 批处理大小
	flushInterval time.Duration            // 刷新间隔
	stopCh        chan struct{}            // 停止通道
	// autoFlushPaused 为 true 时跳过定时 flush。事务路径依赖显式 Flush/Commit 同步传播错误，
	// 必须暂停后台 timer，避免与事务 commit/rollback 竞态。
	autoFlushPaused atomic.Bool
}

// NewBufferedWriter 创建缓冲写入器函数
func NewBufferedWriter(db *sql.DB, identity *entity.TableIdentity, batchSize int, flushInterval time.Duration) *BufferedWriter { // 创建缓冲写入器实例
	w := &BufferedWriter{ // 初始化写入器
		writer:        NewBatchWriter(db, identity, batchSize),      // 创建批量写入器
		buffer:        make([]map[string]interface{}, 0, batchSize), // 创建缓冲区
		batchSize:     batchSize,                                    // 设置批处理大小
		flushInterval: flushInterval,                                // 设置刷新间隔
		stopCh:        make(chan struct{}),                          // 创建停止通道
	}

	// 启动定时刷新协程
	go w.flushLoop() // 启动刷新循环

	return w // 返回写入器
}

// NewBufferedWriterWithSchema 创建带schema的缓冲写入器函数（推荐，确保数据写入正确的schema）
func NewBufferedWriterWithSchema(db *sql.DB, identity *entity.TableIdentity, batchSize int, flushInterval time.Duration, schema string) *BufferedWriter { // 创建带schema的缓冲写入器实例
	w := &BufferedWriter{ // 初始化写入器
		writer:        NewBatchWriterWithSchema(db, identity, batchSize, schema), // 创建带schema的批量写入器
		buffer:        make([]map[string]interface{}, 0, batchSize),              // 创建缓冲区
		batchSize:     batchSize,                                                 // 设置批处理大小
		flushInterval: flushInterval,                                             // 设置刷新间隔
		stopCh:        make(chan struct{}),                                       // 创建停止通道
	}

	// 启动定时刷新协程
	go w.flushLoop() // 启动刷新循环

	return w // 返回写入器
}

// NewBufferedWriterWithConn 创建使用固定连接的缓冲写入器函数
// 使用固定连接可确保 SET SESSION 变量（如 FOREIGN_KEY_CHECKS=0）在整个写入过程中生效
func NewBufferedWriterWithConn(conn *sql.Conn, identity *entity.TableIdentity, batchSize int, flushInterval time.Duration, schema string) *BufferedWriter { // 创建使用固定连接的缓冲写入器实例
	w := &BufferedWriter{ // 初始化写入器
		writer:        NewBatchWriterWithConn(conn, identity, batchSize, schema), // 创建使用固定连接的批量写入器
		buffer:        make([]map[string]interface{}, 0, batchSize),              // 创建缓冲区
		batchSize:     batchSize,                                                 // 设置批处理大小
		flushInterval: flushInterval,                                             // 设置刷新间隔
		stopCh:        make(chan struct{}),                                       // 创建停止通道
	}

	// 启动定时刷新协程
	go w.flushLoop() // 启动刷新循环

	return w // 返回写入器
}

// SetExecutor 切换底层 BatchWriter 的 SQL 执行器。
func (w *BufferedWriter) SetExecutor(db SQLExecutor) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		w.writer.SetExecutor(db)
	}
}

// PauseAutoFlush 暂停后台定时 flush（事务路径在 Begin 时调用）。
func (w *BufferedWriter) PauseAutoFlush() {
	w.autoFlushPaused.Store(true)
}

// ResumeAutoFlush 恢复后台定时 flush（事务 Commit/Rollback 完成后调用）。
func (w *BufferedWriter) ResumeAutoFlush() {
	w.autoFlushPaused.Store(false)
}

// DiscardBuffer 丢弃尚未 flush 的缓冲行（事务回滚时使用）。
func (w *BufferedWriter) DiscardBuffer() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = make([]map[string]interface{}, 0, w.batchSize)
}

// Write 写入数据到缓冲区方法
func (w *BufferedWriter) Write(row map[string]interface{}) error { // 写入数据到缓冲区
	w.mu.Lock()                                 // 获取锁
	w.buffer = append(w.buffer, row)            // 添加到缓冲区
	shouldFlush := len(w.buffer) >= w.batchSize // 判断是否需要刷新
	w.mu.Unlock()                               // 释放锁

	if shouldFlush { // 如果需要刷新
		return w.Flush() // 执行刷新
	}
	return nil // 返回成功
}

// Flush 刷新缓冲区方法。WriteBatch 失败时把已取出的行 prepend 回 buffer，避免静默丢行。
func (w *BufferedWriter) Flush() error { // 刷新缓冲区
	w.mu.Lock()             // 获取锁
	if len(w.buffer) == 0 { // 如果缓冲区为空
		w.mu.Unlock() // 释放锁
		return nil    // 返回成功
	}
	rows := w.buffer                                          // 获取缓冲区数据
	w.buffer = make([]map[string]interface{}, 0, w.batchSize) // 重置缓冲区
	w.mu.Unlock()                                             // 释放锁

	if err := w.writer.WriteBatch(context.Background(), rows); err != nil {
		w.mu.Lock()
		w.buffer = append(rows, w.buffer...)
		w.mu.Unlock()
		return err
	}
	return nil
}

// flushLoop 定时刷新循环。事务路径会 PauseAutoFlush，使 timer 分支跳过，错误改由显式 Flush 同步返回。
func (w *BufferedWriter) flushLoop() { // 定时刷新循环
	ticker := time.NewTicker(w.flushInterval) // 创建定时器
	defer ticker.Stop()                       // 延迟停止定时器

	for { // 无限循环
		select { // 多路复用
		case <-ticker.C: // 定时器触发
			if w.autoFlushPaused.Load() {
				continue
			}
			if err := w.Flush(); err != nil {
				logger.Error("buffered writer auto-flush failed: %v", err)
			}
		case <-w.stopCh: // 停止信号
			if err := w.Flush(); err != nil {
				logger.Error("buffered writer final flush failed: %v", err)
			}
			return // 返回
		}
	}
}

// Close 关闭写入器方法
func (w *BufferedWriter) Close() error { // 关闭写入器
	close(w.stopCh)  // 关闭停止通道
	return w.Flush() // 执行最后一次刷新
}

// EnableUpsert 转发到底层 BatchWriter，让缓冲 INSERT 走 upsert 语义。
// 增量同步路径必须调用；全量同步路径使用 EnablePlainInsert。
func (w *BufferedWriter) EnableUpsert() {
	if w.writer != nil {
		w.writer.EnableUpsert()
	}
}

// EnablePlainInsert 转发到底层 BatchWriter，让缓冲 INSERT 走普通 INSERT 语义。
// 全量同步路径统一调用。
func (w *BufferedWriter) EnablePlainInsert() {
	if w.writer != nil {
		w.writer.EnablePlainInsert()
	}
}

// GetSQLBuilder 获取SQL构建器方法（用于增量同步）
func (w *BatchWriter) GetSQLBuilder() *SQLBuilder { // 获取SQL构建器
	return w.sqlBuilder // 返回SQL构建器
}
