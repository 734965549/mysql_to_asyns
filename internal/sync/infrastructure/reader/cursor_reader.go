package reader

import (
	"context"
	"database/sql"
	"fmt"
	"mysql-to-async/internal/metadata/domain/entity"
	"strings"
)

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
	if r.rows == nil {
		var colParts []string
		for _, col := range r.identity.Columns {
			colParts = append(colParts, "`"+col.Name+"`")
		}
		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", strings.Join(colParts, ", "), r.schema, r.table)
		rows, err := r.db.QueryContext(ctx, query)
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
			val := values[j]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
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
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		// 创建扫描切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 转换为map
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
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
		colParts = append(colParts, "`"+col.Name+"`")
	}
	columns := strings.Join(colParts, ", ")

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` < ?",
		columns, r.schema, r.table, r.pkColumn, r.pkColumn)
	rows, err := r.db.QueryContext(ctx, query, minID, maxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// ReadBatchByKeys 批量读取数据（Keyset Pagination，支持单列和复合主键）
func (r *RangeShardingReader) ReadBatchByKeys(ctx context.Context, lastID interface{}, limit int64) ([]map[string]interface{}, error) {
	var colParts []string
	for _, col := range r.identity.Columns {
		colParts = append(colParts, "`"+col.Name+"`")
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

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanRows(rows)
}

// ReadByRange 按范围读取
func (r *RangeShardingReader) ReadByRange(ctx context.Context, startID, endID int64) ([]map[string]interface{}, error) {
	return r.ReadBatch(ctx, startID, endID)
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
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
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
