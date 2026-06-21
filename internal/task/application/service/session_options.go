package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"mysql-to-sync/pkg/logger"
)

const (
	fullSyncDisableChecksSQL   = "SET SESSION FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0"
	fullSyncRestoreChecksSQL   = "SET SESSION FOREIGN_KEY_CHECKS=1, UNIQUE_CHECKS=1"
	fullSyncVerifyChecksSQL    = "SELECT @@SESSION.FOREIGN_KEY_CHECKS, @@SESSION.UNIQUE_CHECKS"
	fullSyncLockWaitTimeoutSQL = "SET SESSION innodb_lock_wait_timeout=300"
)

func disableFullSyncWriteSession(ctx context.Context, conn *sql.Conn, label string) error {
	if conn == nil {
		return fmt.Errorf("target write connection is nil for %s", label)
	}
	if _, err := conn.ExecContext(ctx, fullSyncDisableChecksSQL); err != nil {
		return fmt.Errorf("disable foreign key and unique checks for %s: %w", label, err)
	}

	var foreignKeyChecks, uniqueChecks int
	if err := conn.QueryRowContext(ctx, fullSyncVerifyChecksSQL).Scan(&foreignKeyChecks, &uniqueChecks); err != nil {
		return fmt.Errorf("verify disabled target write session for %s: %w", label, err)
	}
	if foreignKeyChecks != 0 || uniqueChecks != 0 {
		return fmt.Errorf("target write session for %s still has FOREIGN_KEY_CHECKS=%d UNIQUE_CHECKS=%d", label, foreignKeyChecks, uniqueChecks)
	}
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

func restoreFullSyncWriteSession(conn *sql.Conn, label string) {
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, fullSyncRestoreChecksSQL); err != nil {
		logger.Warn("[Task] Failed to restore target write session for %s: %v", label, err)
	}
}
