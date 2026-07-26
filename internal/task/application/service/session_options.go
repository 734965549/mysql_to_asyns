package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"mysql-to-sync/pkg/logger"
)

const (
	fullSyncDisableForeignKeyChecksSQL = "SET @@SESSION.FOREIGN_KEY_CHECKS=0"
	fullSyncDisableUniqueChecksSQL     = "SET @@SESSION.UNIQUE_CHECKS=0"
	fullSyncRestoreForeignKeyChecksSQL = "SET @@SESSION.FOREIGN_KEY_CHECKS=1"
	fullSyncRestoreUniqueChecksSQL     = "SET @@SESSION.UNIQUE_CHECKS=1"
	fullSyncVerifyChecksSQL            = "SELECT @@SESSION.FOREIGN_KEY_CHECKS, @@SESSION.UNIQUE_CHECKS"
	fullSyncLockWaitTimeoutSQL         = "SET SESSION innodb_lock_wait_timeout=300"
	fullSyncDisableBinlogSQL           = "SET SESSION sql_log_bin=0"
	fullSyncRestoreBinlogSQL           = "SET SESSION sql_log_bin=1"
)

// disableFullSyncWriteSession applies bulk-load optimizations for full sync.
// Full sync uses plain INSERT and requires an empty target; UNIQUE_CHECKS=0 is
// not a conflict-detection mechanism for non-empty targets.
func disableFullSyncWriteSession(ctx context.Context, conn *sql.Conn, label string, skipBinlog bool) error {
	if conn == nil {
		return fmt.Errorf("target write connection is nil for %s", label)
	}
	if _, err := conn.ExecContext(ctx, fullSyncDisableForeignKeyChecksSQL); err != nil {
		return fmt.Errorf("disable foreign key checks for %s: %w", label, err)
	}
	configured := false
	defer func() {
		// sql.Conn.Close returns the physical connection to database/sql's pool,
		// while go-sql-driver/mysql does not reset session variables on checkout.
		// Restore partial configuration before an error can leak it to a later user.
		if !configured {
			restoreFullSyncWriteSession(conn, label, false)
		}
	}()
	if _, err := conn.ExecContext(ctx, fullSyncDisableUniqueChecksSQL); err != nil {
		logger.Warn("[Task] Failed to disable unique checks for %s; continuing because this is only a write optimization: %v", label, err)
	}

	var foreignKeyChecks, uniqueChecks int
	if err := conn.QueryRowContext(ctx, fullSyncVerifyChecksSQL).Scan(&foreignKeyChecks, &uniqueChecks); err != nil {
		return fmt.Errorf("verify disabled target write session for %s: %w", label, err)
	}
	if foreignKeyChecks != 0 {
		return fmt.Errorf("target write session for %s still has FOREIGN_KEY_CHECKS=%d UNIQUE_CHECKS=%d", label, foreignKeyChecks, uniqueChecks)
	}
	if uniqueChecks != 0 {
		logger.Warn("[Task] Target write session for %s still has UNIQUE_CHECKS=%d; continuing because foreign key checks are disabled", label, uniqueChecks)
	}

	if skipBinlog {
		if _, err := conn.ExecContext(ctx, fullSyncDisableBinlogSQL); err != nil {
			return fmt.Errorf("disable binlog for %s: %w", label, err)
		}
	}

	configured = true
	return nil
}

func setFullSyncLockWaitTimeout(ctx context.Context, conn *sql.Conn, label string) error {
	if conn == nil {
		return fmt.Errorf("target write connection is nil for %s", label)
	}
	if _, err := conn.ExecContext(ctx, fullSyncLockWaitTimeoutSQL); err != nil {
		return fmt.Errorf("set lock wait timeout for %s: %w", label, err)
	}
	return nil
}

func restoreFullSyncWriteSession(conn *sql.Conn, label string, skipBinlog bool) {
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, fullSyncRestoreForeignKeyChecksSQL); err != nil {
		logger.Warn("[Task] Failed to restore foreign key checks for %s: %v", label, err)
	}
	if _, err := conn.ExecContext(ctx, fullSyncRestoreUniqueChecksSQL); err != nil {
		logger.Warn("[Task] Failed to restore unique checks for %s: %v", label, err)
	}

	if skipBinlog {
		if _, err := conn.ExecContext(ctx, fullSyncRestoreBinlogSQL); err != nil {
			logger.Warn("[Task] Failed to restore sql_log_bin for %s: %v", label, err)
		}
	}
}
