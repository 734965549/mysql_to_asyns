// Package fullload 实现 P2.3: staging 表命名和生命周期管理。
package fullload

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

const mysqlMaxIdentifierLen = 64

// stagingTableName 生成 staging 表名: __mts_staging_<原表名>_<attemptID>
// 超过 MySQL 64 字符限制时改用短哈希名，避免 DDL 失败。
func stagingTableName(baseTable string, attemptID int) string {
	name := fmt.Sprintf("__mts_staging_%s_%d", baseTable, attemptID)
	if len(name) <= mysqlMaxIdentifierLen {
		return name
	}
	sum := sha256.Sum256([]byte(baseTable))
	short := hex.EncodeToString(sum[:8])
	return fmt.Sprintf("__mts_s_%s_%d", short, attemptID)
}

func oldBackupTableName(baseTable string, ts string) string {
	name := fmt.Sprintf("__mts_old_%s_%s", baseTable, ts)
	if len(name) <= mysqlMaxIdentifierLen {
		return name
	}
	sum := sha256.Sum256([]byte(baseTable))
	short := hex.EncodeToString(sum[:8])
	return fmt.Sprintf("__mts_o_%s_%s", short, ts)
}

// createStagingTable 在目标库中创建空的 staging 表,结构与目标表完全一致。
func createStagingTable(ctx context.Context, db *sql.DB, targetSchema, targetTable string, attemptID int) error {
	stagingTable := stagingTableName(targetTable, attemptID)
	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s",
		quoteIdentifier(targetSchema), quoteIdentifier(stagingTable),
		quoteIdentifier(targetSchema), quoteIdentifier(targetTable),
	)
	_, err := db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("create staging table %s.%s: %w", targetSchema, stagingTable, err)
	}
	return nil
}

// truncateStagingTable 清空 staging 表,用于同一 attempt 内清理残留。
func truncateStagingTable(ctx context.Context, db *sql.DB, targetSchema, targetTable string, attemptID int) error {
	stagingTable := stagingTableName(targetTable, attemptID)
	truncateSQL := fmt.Sprintf(
		"TRUNCATE TABLE %s.%s",
		quoteIdentifier(targetSchema), quoteIdentifier(stagingTable),
	)
	_, err := db.ExecContext(ctx, truncateSQL)
	if err != nil {
		return fmt.Errorf("truncate staging table %s.%s: %w", targetSchema, stagingTable, err)
	}
	return nil
}

// publishStagingTable 将 staging 表原子地发布到最终表(RENAME 操作)。
func publishStagingTable(ctx context.Context, db *sql.DB, targetSchema, targetTable string, attemptID int) error {
	stagingTable := stagingTableName(targetTable, attemptID)
	oldTableName := oldBackupTableName(targetTable, timestampSuffix())

	finalTableExists, err := tableExists(ctx, db, targetSchema, targetTable)
	if err != nil {
		return fmt.Errorf("check final table existence: %w", err)
	}

	var renameSQL string
	if finalTableExists {
		renameSQL = fmt.Sprintf(
			"RENAME TABLE %s.%s TO %s.%s, %s.%s TO %s.%s",
			quoteIdentifier(targetSchema), quoteIdentifier(targetTable),
			quoteIdentifier(targetSchema), quoteIdentifier(oldTableName),
			quoteIdentifier(targetSchema), quoteIdentifier(stagingTable),
			quoteIdentifier(targetSchema), quoteIdentifier(targetTable),
		)
	} else {
		renameSQL = fmt.Sprintf(
			"RENAME TABLE %s.%s TO %s.%s",
			quoteIdentifier(targetSchema), quoteIdentifier(stagingTable),
			quoteIdentifier(targetSchema), quoteIdentifier(targetTable),
		)
	}

	_, err = db.ExecContext(ctx, renameSQL)
	if err != nil {
		return fmt.Errorf("publish staging table %s.%s: %w", targetSchema, stagingTable, err)
	}
	return nil
}

// dropStagingTable 删除 staging 表,用于清理失败的 attempt 残留。
func dropStagingTable(ctx context.Context, db *sql.DB, targetSchema, targetTable string, attemptID int) error {
	stagingTable := stagingTableName(targetTable, attemptID)
	dropSQL := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s.%s",
		quoteIdentifier(targetSchema), quoteIdentifier(stagingTable),
	)
	_, err := db.ExecContext(ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("drop staging table %s.%s: %w", targetSchema, stagingTable, err)
	}
	return nil
}

// dropOldBackupTables 清理旧的备份表 `__mts_old_*`,避免积累过多历史表。
func dropOldBackupTables(ctx context.Context, db *sql.DB, targetSchema, targetTable string, keepRecent int) error {
	if keepRecent < 0 {
		keepRecent = 1
	}

	patterns := []string{
		fmt.Sprintf("__mts_old_%s_%%", targetTable),
	}
	sum := sha256.Sum256([]byte(targetTable))
	short := hex.EncodeToString(sum[:8])
	patterns = append(patterns, fmt.Sprintf("__mts_o_%s_%%", short))

	var backupTables []string
	for _, pattern := range patterns {
		query := `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ? ORDER BY TABLE_NAME DESC`
		rows, err := db.QueryContext(ctx, query, targetSchema, pattern)
		if err != nil {
			return fmt.Errorf("query old backup tables: %w", err)
		}
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				rows.Close()
				return fmt.Errorf("scan backup table name: %w", err)
			}
			backupTables = append(backupTables, tableName)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate backup tables: %w", err)
		}
		rows.Close()
	}

	if len(backupTables) <= keepRecent {
		return nil
	}

	toDrop := backupTables[keepRecent:]
	for _, tableName := range toDrop {
		dropSQL := fmt.Sprintf(
			"DROP TABLE IF EXISTS %s.%s",
			quoteIdentifier(targetSchema), quoteIdentifier(tableName),
		)
		if _, err := db.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("drop old backup table %s.%s: %w", targetSchema, tableName, err)
		}
	}
	return nil
}

func timestampSuffix() string {
	return time.Now().Format("20060102_150405")
}

func tableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	query := "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1"
	var exists int
	err := db.QueryRowContext(ctx, query, schema, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check table existence: %w", err)
	}
	return true, nil
}

const (
	stagingTablePrefix = "__mts_staging_"
	oldBackupPrefix    = "__mts_old_"
)

// StagingTableRef 待清理的 staging 表引用（目标 schema + 精确表名）。
type StagingTableRef struct {
	Schema string
	Table  string
}

// CleanupStaleStagingTables 按显式清单清理残留 staging 表。
// refs 为空时不做任何删除（fail-closed，避免全库前缀扫描误删）。
// 兼容旧测试：若传入的是单段 schema 名（Table 为空）则忽略。
func CleanupStaleStagingTables(ctx context.Context, db *sql.DB, refs []StagingTableRef) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("CleanupStaleStagingTables: nil db")
	}
	if len(refs) == 0 {
		return 0, nil
	}

	dropped := 0
	for _, ref := range refs {
		if ref.Schema == "" || ref.Table == "" {
			continue
		}
		exists, err := tableExists(ctx, db, ref.Schema, ref.Table)
		if err != nil {
			return dropped, err
		}
		if !exists {
			continue
		}
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
			quoteIdentifier(ref.Schema), quoteIdentifier(ref.Table))
		if _, err := db.ExecContext(ctx, dropSQL); err != nil {
			return dropped, fmt.Errorf("drop stale staging %s.%s: %w", ref.Schema, ref.Table, err)
		}
		dropped++
	}
	return dropped, nil
}

// CleanupStaleStagingTablesByKeys 兼容旧调用：按 "schema.sourceTable" 扫描 attempt 1..16 的派生 staging 名。
// 生产路径应优先使用 CleanupStaleStagingTables + 持久化精确表名。
func CleanupStaleStagingTablesByKeys(ctx context.Context, db *sql.DB, keys []string) (int, error) {
	var refs []StagingTableRef
	for _, key := range keys {
		schema, table, ok := splitSchemaTable(key)
		if !ok {
			continue
		}
		for attempt := 1; attempt <= 16; attempt++ {
			refs = append(refs, StagingTableRef{Schema: schema, Table: stagingTableName(table, attempt)})
		}
	}
	return CleanupStaleStagingTables(ctx, db, refs)
}

func splitSchemaTable(key string) (schema, table string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			if i == 0 || i == len(key)-1 {
				return "", "", false
			}
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}
