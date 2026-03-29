package reader

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"mysql-to-async/internal/metadata/domain/entity"
	"strings"
	"time"
)

// defaultSelectLimit 当调用方传入 limit<=0 时的兜底，避免生成无 LIMIT 的查询一次拉全表。
const defaultSelectLimit int64 = 1000

func normalizeSelectLimit(limit int64) int64 {
	if limit <= 0 {
		return defaultSelectLimit
	}
	return limit
}

func isConnRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "invalid connection") ||
		strings.Contains(s, "bad connection") ||
		strings.Contains(s, "unexpected packet") ||
		strings.Contains(s, "connection was bad")
}

// drainQueryWithRetry 对池中已失效连接（如超过 wait_timeout）导致的读失败做有限次重试。
func drainQueryWithRetry(ctx context.Context, open func() (*sql.Rows, error), scan func(*sql.Rows) ([]map[string]interface{}, error)) ([]map[string]interface{}, error) {
	const maxAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(25+attempt*25) * time.Millisecond):
			}
		}
		rows, err := open()
		if err != nil {
			lastErr = err
			if isConnRetryable(err) && attempt < maxAttempts-1 {
				continue
			}
			return nil, err
		}
		results, sErr := scan(rows)
		if cErr := rows.Close(); cErr != nil && sErr == nil {
			sErr = cErr
		}
		if sErr != nil {
			lastErr = sErr
			if isConnRetryable(sErr) && attempt < maxAttempts-1 {
				continue
			}
			return nil, sErr
		}
		return results, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("unknown")
	}
	return nil, fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

// selectExprForColumn 构建 SELECT 列表项。JSON/BLOB/TEXT 等原样读取，避免 CAST 在服务端物化整段大字段拖慢 IO；Scan 后经 normalizeScannedValue 处理 []byte。
func selectExprForColumn(col entity.ColumnMeta) string {
	q := "`" + strings.ReplaceAll(col.Name, "`", "``") + "`"
	return q
}

// normalizeScannedValue 将 Scan 结果放入 map：对 []byte 先拷贝再转为 string，避免 database/sql 与驱动复用底层缓冲区导致跨行串数据，并与历史写入行为一致。
func normalizeScannedValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	if b, ok := val.([]byte); ok {
		out := make([]byte, len(b))
		copy(out, b)
		return string(out)
	}
	return val
}

// DataReader 数据读取器接口
type DataReader interface {
	// ReadBatch 批量读取数据
	ReadBatch(ctx context.Context, offset, limit int64) ([]map[string]interface{}, error)
	// ReadBatchByKeys 批量读取数据（基于主键范围，优化深分页）
	ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error)
	// GetTotalCount 获取总行数
	GetTotalCount(ctx context.Context) (int64, error)
}

// CursorReader 无主键表流式读取器（单次全表扫描，避免 OFFSET 深翻页）
type CursorReader struct {
	db       *sql.DB
	schema   string
	table    string
	identity *entity.TableIdentity
	rows     *sql.Rows // 流式游标，第一次 ReadBatch 时打开
	colNames []string  // 列名缓存
}

// NewCursorReader 创建流式读取器
func NewCursorReader(db *sql.DB, schema, table string, identity *entity.TableIdentity) *CursorReader {
	return &CursorReader{
		db:       db,
		schema:   schema,
		table:    table,
		identity: identity,
	}
}

// ReadBatch 批量读取数据（流式：第一次调用打开游标，后续调用继续从游标读取）
// offset 参数在流式模式下忽略（已由游标位置隐含）
func (r *CursorReader) ReadBatch(ctx context.Context, _ /* offset */, limit int64) ([]map[string]interface{}, error) {
	limit = normalizeSelectLimit(limit)
	if r.rows == nil {
		var colParts []string
		for _, col := range r.identity.Columns {
			colParts = append(colParts, selectExprForColumn(col))
		}
		// 添加 LIMIT 子句，避免一次性加载整表到内存
		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT ?", strings.Join(colParts, ", "), r.schema, r.table)
		rows, err := r.db.QueryContext(ctx, query, limit)
		if err != nil {
			return nil, fmt.Errorf("打开流式游标失败: %v, SQL: %s", err, query)
		}
		r.rows = rows
		r.colNames, err = rows.Columns()
		if err != nil {
			return nil, err
		}
	}

	var results []map[string]interface{}
	for i := int64(0); i < limit; i++ {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return nil, err
			}
			_ = r.rows.Close()
			r.rows = nil
			break
		}
		values := make([]interface{}, len(r.colNames))
		valuePtrs := make([]interface{}, len(r.colNames))
		for j := range values {
			valuePtrs[j] = &values[j]
		}
		if err := r.rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for j, col := range r.colNames {
			row[col] = normalizeScannedValue(values[j])
		}
		results = append(results, row)
	}
	return results, nil
}

// ReadBatchByKeys 批量读取数据（无主键表不支持，回退到 ReadBatch）
func (r *CursorReader) ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("ReadBatchByKeys not supported for no-PK tables")
}

// GetTotalCount 获取总行数
func (r *CursorReader) GetTotalCount(ctx context.Context) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", r.schema, r.table)
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// scanRows 扫描行数据
func (r *CursorReader) scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	return scanRowsToMaps(rows)
}

// RangeShardingReader 有主键/唯一键表分片读取器（支持单列和复合主键）
type RangeShardingReader struct {
	db       *sql.DB
	schema   string
	table    string
	identity *entity.TableIdentity
	pkColumn string // 兼容保留，单列时使用
}

// NewRangeShardingReader 创建分片读取器
func NewRangeShardingReader(db *sql.DB, schema, table string, identity *entity.TableIdentity) *RangeShardingReader {
	pkColumn := ""
	if len(identity.IdentifyCols) > 0 {
		pkColumn = identity.IdentifyCols[0]
	}
	return &RangeShardingReader{
		db:       db,
		schema:   schema,
		table:    table,
		identity: identity,
		pkColumn: pkColumn,
	}
}

// ReadBatch 批量读取数据（按范围）
func (r *RangeShardingReader) ReadBatch(ctx context.Context, minID, maxID int64) ([]map[string]interface{}, error) {
	var colParts []string
	for _, col := range r.identity.Columns {
		colParts = append(colParts, selectExprForColumn(col))
	}
	columns := strings.Join(colParts, ", ")

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ?",
		columns, r.schema, r.table, r.pkColumn, r.pkColumn)
	return drainQueryWithRetry(ctx, func() (*sql.Rows, error) {
		return r.db.QueryContext(ctx, query, minID, maxID)
	}, r.scanRows)
}

// ReadBatchByKeys 批量读取数据（Keyset Pagination，支持单列和复合主键）
func (r *RangeShardingReader) ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) {
	limit = normalizeSelectLimit(limit)
	var colParts []string
	for _, col := range r.identity.Columns {
		colParts = append(colParts, selectExprForColumn(col))
	}
	columns := strings.Join(colParts, ", ")

	pkCols := r.identity.IdentifyCols

	var query string
	var args []interface{}

	if len(pkCols) == 1 {
		// 单列主键：WHERE pk > ? ORDER BY pk LIMIT ?
		if lastID == nil {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY `%s` ASC LIMIT ?",
				columns, r.schema, r.table, pkCols[0])
			args = []interface{}{limit}
		} else {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` > ? ORDER BY `%s` ASC LIMIT ?",
				columns, r.schema, r.table, pkCols[0], pkCols[0])
			args = []interface{}{lastID, limit}
		}
	} else {
		// 复合主键：WHERE (col1, col2, ...) > (?, ?, ...) ORDER BY col1, col2 LIMIT ?
		// MySQL 支持行构造器比较，利用索引高效定位
		var bkCols []string
		for _, col := range pkCols {
			bkCols = append(bkCols, "`"+col+"`")
		}
		colList := strings.Join(bkCols, ", ")
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(pkCols)), ", ")

		if lastID == nil {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s ASC LIMIT ?",
				columns, r.schema, r.table, colList)
			args = []interface{}{limit}
		} else {
			lastIDs, ok := lastID.([]interface{})
			if !ok {
				return nil, fmt.Errorf("复合主键 lastID 必须是 []interface{}，实际: %T", lastID)
			}
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE (%s) > (%s) ORDER BY %s ASC LIMIT ?",
				columns, r.schema, r.table, colList, placeholders, colList)
			args = append(append([]interface{}{}, lastIDs...), limit)
		}
	}

	return drainQueryWithRetry(ctx, func() (*sql.Rows, error) {
		return r.db.QueryContext(ctx, query, args...)
	}, r.scanRows)
}

// ReadBatchInRange 批量读取数据（指定范围内，且带 LIMIT）
func (r *RangeShardingReader) ReadBatchInRange(ctx context.Context, startID, endID, limit int64) ([]map[string]interface{}, error) {
	// 必须带 LIMIT：limit 由任务 batch_size 驱动；<=0 时兜底，避免 WHERE 区间内一次扫出全部行。
	limit = normalizeSelectLimit(limit)
	var colParts []string
	for _, col := range r.identity.Columns {
		colParts = append(colParts, selectExprForColumn(col))
	}
	columns := strings.Join(colParts, ", ")

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ? ORDER BY `%s` ASC LIMIT ?",
		columns, r.schema, r.table, r.pkColumn, r.pkColumn, r.pkColumn)
	return drainQueryWithRetry(ctx, func() (*sql.Rows, error) {
		return r.db.QueryContext(ctx, query, startID, endID, limit)
	}, r.scanRows)
}

// ReadByRange 按范围读取
func (r *RangeShardingReader) ReadByRange(ctx context.Context, startID, endID int64) ([]map[string]interface{}, error) {
	return r.ReadBatch(ctx, startID, endID)
}

// OpenRangeStream 在指定连接上打开一次覆盖整个范围的流式查询。
// 调用方负责调用 rows.Close()。
// 使用独立连接而非连接池，避免多 worker 并发时连接池竞争和源库 I/O 抖动。
func (r *RangeShardingReader) OpenRangeStream(conn *sql.Conn, ctx context.Context, minID, maxID int64) (*sql.Rows, []string, error) {
	var colParts []string
	for _, col := range r.identity.Columns {
		colParts = append(colParts, selectExprForColumn(col))
	}
	columns := strings.Join(colParts, ", ")
	query := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ? ORDER BY `%s` ASC",
		columns, r.schema, r.table, r.pkColumn, r.pkColumn, r.pkColumn,
	)
	rows, err := conn.QueryContext(ctx, query, minID, maxID)
	if err != nil {
		return nil, nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, nil, err
	}
	return rows, cols, nil
}

// GetTotalCount 获取总行数
func (r *RangeShardingReader) GetTotalCount(ctx context.Context) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", r.schema, r.table)
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// scanRows 扫描行数据
func (r *RangeShardingReader) scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	return scanRowsToMaps(rows)
}

func scanRowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = normalizeScannedValue(values[i])
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// NewReader 根据表标识创建合适的读取器
func NewReader(db *sql.DB, schema, table string, identity *entity.TableIdentity) DataReader {
	if identity.Strategy == entity.FullColumnsStrategy {
		return NewCursorReader(db, schema, table, identity)
	}
	return NewRangeShardingReader(db, schema, table, identity)
}
