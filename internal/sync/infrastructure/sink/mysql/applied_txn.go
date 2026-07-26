package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"mysql-to-sync/pkg/binlog"
)

// 目标库内与业务数据同事务提交的源位点表，用于崩溃重放去重（尤其无主键 INSERT）。
const appliedTxnTableName = "_mts_applied_txn"

func (s *MySQLSink) ensureMetaSchemaLocked(dbMapping map[string]string) {
	if s.metaSchema != "" || len(dbMapping) == 0 {
		return
	}
	targets := make([]string, 0, len(dbMapping))
	seen := make(map[string]struct{}, len(dbMapping))
	for _, tgt := range dbMapping {
		tgt = strings.TrimSpace(tgt)
		if tgt == "" {
			continue
		}
		if _, ok := seen[tgt]; ok {
			continue
		}
		seen[tgt] = struct{}{}
		targets = append(targets, tgt)
	}
	if len(targets) == 0 {
		return
	}
	sort.Strings(targets)
	s.metaSchema = targets[0]
}

func (s *MySQLSink) ensureAppliedTxnTable(ctx context.Context) error {
	s.mu.RLock()
	metaSchema := s.metaSchema
	writeConn := s.writeConn
	activeTx := s.activeTx
	closed := s.closed
	ready := s.metaTableReady
	s.mu.RUnlock()

	if closed {
		return fmt.Errorf("mysql sink is closed")
	}
	if writeConn == nil {
		return fmt.Errorf("mysql sink is not open")
	}
	if metaSchema == "" {
		return fmt.Errorf("mysql sink meta schema is not configured (PrepareTables required)")
	}
	if ready {
		return nil
	}
	// DDL 会隐式提交，且 writeConn 可能是池中唯一连接；禁止在活跃业务事务期间建表。
	if activeTx != nil {
		return fmt.Errorf("applied txn table %s.%s is not ready; cannot DDL during active transaction", metaSchema, appliedTxnTableName)
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
			"`task_id` VARCHAR(191) NOT NULL,"+
			"`binlog_file` VARCHAR(255) NOT NULL,"+
			"`binlog_pos` BIGINT UNSIGNED NOT NULL,"+
			"`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
			"PRIMARY KEY (`task_id`)"+
			") ENGINE=InnoDB",
		metaSchema, appliedTxnTableName,
	)
	if _, err := writeConn.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create applied txn table %s.%s: %w", metaSchema, appliedTxnTableName, err)
	}

	s.mu.Lock()
	s.metaTableReady = true
	s.mu.Unlock()
	return nil
}

// HasAppliedTxn 查询目标库是否已提交过该源事务位点（或更新的位点）。
func (s *MySQLSink) HasAppliedTxn(ctx context.Context, taskID string, pos mysql.Position) (bool, error) {
	if taskID == "" {
		return false, fmt.Errorf("task id is required")
	}
	if pos.Name == "" {
		return false, fmt.Errorf("binlog position name is required")
	}
	if err := s.ensureAppliedTxnTable(ctx); err != nil {
		return false, err
	}

	s.mu.RLock()
	metaSchema := s.metaSchema
	writeConn := s.writeConn
	s.mu.RUnlock()

	query := fmt.Sprintf(
		"SELECT `binlog_file`, `binlog_pos` FROM `%s`.`%s` WHERE `task_id` = ?",
		metaSchema, appliedTxnTableName,
	)
	var file string
	var storedPos uint64
	err := writeConn.QueryRowContext(ctx, query, taskID).Scan(&file, &storedPos)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query applied txn position: %w", err)
	}
	stored := mysql.Position{Name: file, Pos: uint32(storedPos)}
	return binlog.ComparePosition(stored, pos) >= 0, nil
}

// MarkAppliedTxn 在活跃目标事务内写入源提交位点，与业务数据同事务提交。
func (s *MySQLSink) MarkAppliedTxn(ctx context.Context, taskID string, pos mysql.Position) error {
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	if pos.Name == "" {
		return fmt.Errorf("binlog position name is required")
	}
	if err := s.ensureAppliedTxnTable(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	metaSchema := s.metaSchema
	activeTx := s.activeTx
	s.mu.RUnlock()
	if activeTx == nil {
		return fmt.Errorf("mysql sink transaction is not active")
	}

	stmt := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (`task_id`, `binlog_file`, `binlog_pos`) VALUES (?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE `binlog_file` = VALUES(`binlog_file`), `binlog_pos` = VALUES(`binlog_pos`)",
		metaSchema, appliedTxnTableName,
	)
	if _, err := activeTx.ExecContext(ctx, stmt, taskID, pos.Name, pos.Pos); err != nil {
		return fmt.Errorf("mark applied txn position: %w", err)
	}
	return nil
}
