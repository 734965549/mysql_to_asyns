package writer

import (
	"strings"
	"testing"

	"mysql-to-sync/internal/metadata/domain/entity"
)

func createTestIdentity(strategy entity.IdentityStrategy, tableName string, columns []entity.ColumnMeta, identifyCols []string) *entity.TableIdentity {
	return &entity.TableIdentity{
		TableName:    tableName,
		Strategy:     strategy,
		IdentifyCols: identifyCols,
		Columns:      columns,
		HasPK:        strategy == entity.PKStrategy,
		HasUK:        strategy == entity.UKStrategy,
	}
}

func TestSQLBuilder_BuildInsert(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
		{Name: "email", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"id":    1,
		"name":  "John",
		"email": "john@example.com",
	}

	query, args := builder.BuildInsert(row)

	// 验证使用 INSERT ... ON DUPLICATE KEY UPDATE
	if !strings.Contains(query, "INSERT INTO") {
		t.Error("expected INSERT INTO statement")
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Error("expected ON DUPLICATE KEY UPDATE clause")
	}

	// 验证表名
	if !strings.Contains(query, "users") {
		t.Error("expected table name 'users' in query")
	}

	// 验证参数数量
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestSQLBuilder_BuildInsert_ColumnOrder(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "col1", IsPrimaryKey: true},
		{Name: "col2", IsPrimaryKey: false},
		{Name: "col3", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "test_table", columns, []string{"col1"})
	builder := NewSQLBuilder(identity)

	// 行数据顺序与列定义不同
	row := map[string]interface{}{
		"col3": "value3",
		"col1": "value1",
		"col2": "value2",
	}

	query, args := builder.BuildInsert(row)

	// 验证列顺序按照 identity.Columns 的顺序
	if !strings.Contains(query, "`col1`, `col2`, `col3`") {
		t.Errorf("columns should be in order: col1, col2, col3, got: %s", query)
	}

	// 验证参数顺序
	if args[0] != "value1" || args[1] != "value2" || args[2] != "value3" {
		t.Errorf("args should be in order [value1, value2, value3], got: %v", args)
	}
}

func TestSQLBuilder_BuildInsertOnDuplicate(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
		{Name: "email", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"id":    1,
		"name":  "John",
		"email": "john@example.com",
	}

	query, args := builder.BuildInsertOnDuplicate(row)

	// 验证使用 INSERT ... ON DUPLICATE KEY UPDATE
	if !strings.Contains(query, "INSERT INTO") {
		t.Error("expected INSERT INTO statement")
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Error("expected ON DUPLICATE KEY UPDATE clause")
	}

	// 验证更新子句不包含主键
	if strings.Contains(query, "`id` = VALUES(`id`)") {
		t.Error("should not update primary key column")
	}

	// 验证更新子句包含非主键列
	if !strings.Contains(query, "`name` = VALUES(`name`)") {
		t.Error("expected name column in update clause")
	}
	if !strings.Contains(query, "`email` = VALUES(`email`)") {
		t.Error("expected email column in update clause")
	}

	// 验证参数数量
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestSQLBuilder_BuildUpdate(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
		{Name: "email", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"id":    1,
		"name":  "John Updated",
		"email": "john.updated@example.com",
	}

	query, args := builder.BuildUpdate(row)

	// 验证 UPDATE 语句
	if !strings.Contains(query, "UPDATE `users` SET") {
		t.Errorf("expected UPDATE statement, got: %s", query)
	}

	// 验证 SET 子句不包含主键（检查SET和WHERE之间的部分）
	setPart := strings.Split(query, "WHERE")[0]
	if strings.Contains(setPart, "id = ?") {
		t.Error("should not update primary key column in SET clause")
	}

	// 验证 SET 子句包含非主键列
	if !strings.Contains(query, "`name` = ?") {
		t.Error("expected name column in SET clause")
	}
	if !strings.Contains(query, "`email` = ?") {
		t.Error("expected email column in SET clause")
	}

	// 验证 WHERE 子句
	if !strings.Contains(query, "WHERE id = ?") {
		t.Error("expected WHERE clause with id")
	}

	// 验证参数顺序：先 SET 参数，后 WHERE 参数
	if len(args) != 3 {
		t.Errorf("expected 3 args (2 SET + 1 WHERE), got %d", len(args))
	}
}

func TestSQLBuilder_BuildUpdate_CompositeKey(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "user_id", IsPrimaryKey: true},
		{Name: "role_id", IsPrimaryKey: true},
		{Name: "assigned_at", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "user_roles", columns, []string{"user_id", "role_id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"user_id":     1,
		"role_id":     2,
		"assigned_at": "2024-01-01",
	}

	query, args := builder.BuildUpdate(row)

	// 验证 WHERE 子句包含复合主键
	if !strings.Contains(query, "WHERE user_id = ? AND role_id = ?") {
		t.Errorf("expected WHERE clause with composite key, got: %s", query)
	}

	// 验证参数数量
	if len(args) != 3 {
		t.Errorf("expected 3 args (1 SET + 2 WHERE), got %d", len(args))
	}
}

func TestSQLBuilder_BuildDelete(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"id":   1,
		"name": "John",
	}

	query, args := builder.BuildDelete(row)

	// 验证 DELETE 语句
	if !strings.Contains(query, "DELETE FROM `users` WHERE id = ?") {
		t.Errorf("unexpected delete query: %s", query)
	}

	// 验证参数
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
	if args[0] != 1 {
		t.Errorf("expected arg 1, got %v", args[0])
	}
}

func TestSQLBuilder_BuildDelete_CompositeKey(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "order_id", IsPrimaryKey: true},
		{Name: "product_id", IsPrimaryKey: true},
	}
	identity := createTestIdentity(entity.PKStrategy, "order_items", columns, []string{"order_id", "product_id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"order_id":   "ORD001",
		"product_id": "PROD001",
	}

	query, args := builder.BuildDelete(row)

	// 验证 DELETE 语句
	if !strings.Contains(query, "DELETE FROM `order_items` WHERE") {
		t.Errorf("expected DELETE FROM order_items WHERE, got: %s", query)
	}
	if !strings.Contains(query, "order_id = ? AND product_id = ?") {
		t.Errorf("expected composite key in WHERE clause, got: %s", query)
	}

	// 验证参数
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestSQLBuilder_BuildBatchInsert(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
		{"id": 3, "name": "Bob"},
	}

	query, args := builder.BuildBatchInsert(rows)

	// 验证 INSERT IGNORE
	if !strings.Contains(query, "INSERT IGNORE INTO `users`") {
		t.Errorf("expected INSERT IGNORE INTO users, got: %s", query)
	}

	// 验证列名
	if !strings.Contains(query, "(`id`, `name`)") {
		t.Errorf("expected columns (id, name), got: %s", query)
	}

	// 验证值占位符
	if !strings.Contains(query, "(?, ?), (?, ?), (?, ?)") {
		t.Errorf("expected 3 row placeholders, got: %s", query)
	}

	// 验证参数数量
	if len(args) != 6 {
		t.Errorf("expected 6 args (3 rows * 2 cols), got %d", len(args))
	}
}

func TestSQLBuilder_BuildBatchInsert_Empty(t *testing.T) {
	identity := createTestIdentity(entity.PKStrategy, "users", []entity.ColumnMeta{}, []string{})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{}

	query, args := builder.BuildBatchInsert(rows)

	if query != "" {
		t.Errorf("expected empty query for empty rows, got: %s", query)
	}
	if args != nil {
		t.Errorf("expected nil args for empty rows, got: %v", args)
	}
}

func TestSQLBuilder_BuildBatchInsert_SingleRow(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "value", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "test", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{
		{"id": 1, "value": "test"},
	}

	query, args := builder.BuildBatchInsert(rows)

	// 验证单行插入
	if !strings.Contains(query, "INSERT IGNORE INTO `test` (`id`, `value`) VALUES (?, ?)") {
		t.Errorf("expected single row insert, got: %s", query)
	}

	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestSQLBuilder_GetStrategyName(t *testing.T) {
	tests := []struct {
		name         string
		strategy     entity.IdentityStrategy
		expectedName string
	}{
		{"PK Strategy", entity.PKStrategy, "PK_STRATEGY"},
		{"UK Strategy", entity.UKStrategy, "UK_STRATEGY"},
		{"Full Columns Strategy", entity.FullColumnsStrategy, "FULL_COLUMNS_STRATEGY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := createTestIdentity(tt.strategy, "test", []entity.ColumnMeta{}, []string{})
			builder := NewSQLBuilder(identity)
			if builder.GetStrategyName() != tt.expectedName {
				t.Errorf("expected strategy name %s, got %s", tt.expectedName, builder.GetStrategyName())
			}
		})
	}
}

func TestSQLBuilder_BuildInsert_PartialRow(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
		{Name: "email", IsPrimaryKey: false},
		{Name: "phone", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	// 只提供部分列
	row := map[string]interface{}{
		"id":   1,
		"name": "John",
		// email 和 phone 缺失
	}

	query, args := builder.BuildInsert(row)

	// 验证只包含提供的列
	if !strings.Contains(query, "id") || !strings.Contains(query, "name") {
		t.Error("query should contain id and name columns")
	}

	// 验证参数数量
	if len(args) != 2 {
		t.Errorf("expected 2 args for partial row, got %d", len(args))
	}
}

func TestSQLBuilder_BuildUpdate_OnlyPrimaryKey(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
	}
	identity := createTestIdentity(entity.PKStrategy, "simple", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"id": 1,
	}

	query, args := builder.BuildUpdate(row)

	// 只有主键时，SET 子句应该为空，但SQL语法要求SET后面至少有一个列
	// 实际实现中，如果没有非主键列，SET子句会是空的
	// 验证查询包含WHERE子句
	if !strings.Contains(query, "WHERE id = ?") {
		t.Errorf("expected WHERE clause with id, got: %s", query)
	}

	// 参数应该只有 WHERE 参数
	if len(args) != 1 {
		t.Errorf("expected 1 arg for WHERE clause, got %d", len(args))
	}
}

func TestSQLBuilder_BuildDelete_WithFullColumnsStrategy(t *testing.T) {
	// 测试无主键表的删除（使用全列匹配）
	columns := []entity.ColumnMeta{
		{Name: "message", IsPrimaryKey: false},
		{Name: "level", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.FullColumnsStrategy, "logs", columns, []string{"message", "level"})
	builder := NewSQLBuilder(identity)

	row := map[string]interface{}{
		"message": "test log",
		"level":   "INFO",
	}

	query, args := builder.BuildDelete(row)

	// 验证 DELETE 语句包含 LIMIT 1
	if !strings.Contains(query, "DELETE FROM `logs` WHERE") {
		t.Errorf("expected DELETE FROM logs WHERE, got: %s", query)
	}
	if !strings.Contains(query, "LIMIT 1") {
		t.Error("expected LIMIT 1 for full columns strategy")
	}

	// 验证参数
	if len(args) != 2 {
		t.Errorf("expected 2 args, got %d", len(args))
	}
}

func TestSQLBuilder_BuildBatchInsert_LargeBatch(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "value", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "test", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	// 创建大批量数据
	rows := make([]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		rows[i] = map[string]interface{}{
			"id":    i + 1,
			"value": "test",
		}
	}

	query, args := builder.BuildBatchInsert(rows)

	// 验证查询不为空
	if query == "" {
		t.Error("query should not be empty for large batch")
	}

	// 验证参数数量
	if len(args) != 200 {
		t.Errorf("expected 200 args (100 rows * 2 cols), got %d", len(args))
	}
}

func TestNewSQLBuilder(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
	}
	identity := createTestIdentity(entity.PKStrategy, "test", columns, []string{"id"})

	builder := NewSQLBuilder(identity)

	if builder == nil {
		t.Fatal("builder should not be nil")
	}
	if builder.identity == nil {
		t.Error("builder identity should not be nil")
	}
	if builder.matchStrategy == nil {
		t.Error("builder matchStrategy should not be nil")
	}
}

// === 修复 10/11：BuildBatchUpsert 行为分流 ===

// TestSQLBuilder_BuildBatchUpsert_PKTableUsesUpsert PK 表应使用 ON DUPLICATE KEY UPDATE，
// 且只更新非主键列，保证同一 PK 的后续 INSERT 事件能覆盖旧值。
func TestSQLBuilder_BuildBatchUpsert_PKTableUsesUpsert(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "name", IsPrimaryKey: false},
		{Name: "email", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "users", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{
		{"id": 1, "name": "alice", "email": "a@x"},
		{"id": 2, "name": "bob", "email": "b@x"},
	}

	query, args := builder.BuildBatchUpsert(rows)

	if !strings.Contains(query, "INSERT INTO") {
		t.Errorf("expected INSERT INTO, got: %s", query)
	}
	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("expected ON DUPLICATE KEY UPDATE, got: %s", query)
	}
	if strings.Contains(query, "`id` = VALUES(`id`)") {
		t.Errorf("PK column should not appear in UPDATE clause, got: %s", query)
	}
	if !strings.Contains(query, "`name` = VALUES(`name`)") {
		t.Errorf("non-PK column 'name' should appear in UPDATE clause, got: %s", query)
	}
	if !strings.Contains(query, "`email` = VALUES(`email`)") {
		t.Errorf("non-PK column 'email' should appear in UPDATE clause, got: %s", query)
	}
	if len(args) != 6 {
		t.Errorf("expected 6 args (2 rows * 3 cols), got %d", len(args))
	}
}

// TestSQLBuilder_BuildBatchUpsert_UKTableUsesUpsert UK 表（无 PK）同样应走 upsert。
func TestSQLBuilder_BuildBatchUpsert_UKTableUsesUpsert(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "email", IsPrimaryKey: false, IsUnique: true},
		{Name: "name", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.UKStrategy, "members", columns, []string{"email"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{{"email": "x@y", "name": "alice"}}
	query, _ := builder.BuildBatchUpsert(rows)

	if !strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("UK table must use upsert, got: %s", query)
	}
}

// TestSQLBuilder_BuildBatchUpsert_FullColumnsDegradedToIgnore 无主键 / 无唯一键表必须退化为 INSERT IGNORE，
// 因为没有冲突键，ON DUPLICATE 没有意义。
func TestSQLBuilder_BuildBatchUpsert_FullColumnsDegradedToIgnore(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "message", IsPrimaryKey: false},
		{Name: "level", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.FullColumnsStrategy, "logs", columns, []string{"message", "level"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{{"message": "hi", "level": "INFO"}}
	query, _ := builder.BuildBatchUpsert(rows)

	if !strings.Contains(query, "INSERT IGNORE") {
		t.Errorf("no-PK/no-UK table must degrade to INSERT IGNORE, got: %s", query)
	}
	if strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("no-PK/no-UK table must NOT emit ON DUPLICATE KEY UPDATE (no conflict key), got: %s", query)
	}
}

// TestSQLBuilder_BuildBatchUpsert_AllPKColumnsFallsBackToIgnore 全部列都是主键时，没有可 UPDATE 的非主键列，
// 应退化为 INSERT IGNORE，避免生成空 SET 子句的非法 SQL。
func TestSQLBuilder_BuildBatchUpsert_AllPKColumnsFallsBackToIgnore(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "a", IsPrimaryKey: true},
		{Name: "b", IsPrimaryKey: true},
	}
	identity := createTestIdentity(entity.PKStrategy, "join_table", columns, []string{"a", "b"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{{"a": 1, "b": 2}}
	query, _ := builder.BuildBatchUpsert(rows)

	if !strings.Contains(query, "INSERT IGNORE") {
		t.Errorf("all-PK table must fall back to INSERT IGNORE, got: %s", query)
	}
}

// TestSQLBuilder_BuildBatchInsert_UnchangedSemantics 守住全量路径继续走 INSERT IGNORE 不变。
func TestSQLBuilder_BuildBatchInsert_UnchangedSemantics(t *testing.T) {
	columns := []entity.ColumnMeta{
		{Name: "id", IsPrimaryKey: true},
		{Name: "v", IsPrimaryKey: false},
	}
	identity := createTestIdentity(entity.PKStrategy, "tbl", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	rows := []map[string]interface{}{{"id": 1, "v": "x"}}
	query, _ := builder.BuildBatchInsert(rows)

	if !strings.Contains(query, "INSERT IGNORE") {
		t.Errorf("BuildBatchInsert must keep INSERT IGNORE semantics for full sync, got: %s", query)
	}
	if strings.Contains(query, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("BuildBatchInsert must NOT emit upsert clause, got: %s", query)
	}
}

// TestSQLBuilder_BuildBatchUpsert_EmptyRowsReturnsEmpty 空切片应安全返回，避免拼出非法 SQL。
func TestSQLBuilder_BuildBatchUpsert_EmptyRowsReturnsEmpty(t *testing.T) {
	columns := []entity.ColumnMeta{{Name: "id", IsPrimaryKey: true}}
	identity := createTestIdentity(entity.PKStrategy, "tbl", columns, []string{"id"})
	builder := NewSQLBuilder(identity)

	query, args := builder.BuildBatchUpsert(nil)
	if query != "" || args != nil {
		t.Errorf("empty rows must return empty query, got query=%q args=%v", query, args)
	}
}
