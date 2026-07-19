package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"mysql-to-sync/internal/config"
	sink "mysql-to-sync/internal/sync/domain/sink"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/pkg/logger"

	// 注册 MySQL 驱动
	_ "github.com/go-sql-driver/mysql"
)

// quoteIdentifierForCount 使用反引号转义标识符（与 internal/sync/fullload/sql.go 行为一致，
// 双写反引号防止注入）。行数对比只读，但表名/schema 可能来自 information_schema，仍需转义。
func quoteIdentifierForCount(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// comparisonTableTask 描述一张待核对表的源/目标 schema+table 映射。
type comparisonTableTask struct {
	sourceSchema string
	sourceTable  string
	targetSchema string
	targetTable  string
}

// resolveComparisonConnections 按任务自身数据库配置（SourceDB/TargetDB）重新创建只读用途的
// 源端和目标端连接。不创建数据库、不修改只读状态、不检查 binlog 配置、不执行任何 DDL/DML；
// 仅连接、Ping、读取元数据与 SELECT COUNT(*)。
//
// 回退规则与 initDatabaseConnections 一致：任务配置没有单独连接信息时采用全局
// 数据源/目标端配置。注意：这里为只读对比，目标端不复用已关闭的同步 runtime。
func (s *TaskService) resolveComparisonConnections(task *taskEntity.SyncTask) (sourceDB, targetDB *sql.DB, err error) {
	resolvedSourceSchema := s.resolveSourceSchema(task)

	sourceConfig := task.Config.SourceDB
	if sourceConfig == nil && s.config != nil {
		sourceConfig = &taskEntity.DatabaseConfig{
			Host:     s.config.Datasource.Host,
			Port:     s.config.Datasource.Port,
			Database: resolvedSourceSchema,
			Username: s.config.Datasource.Username,
			Password: s.config.Datasource.Password,
		}
	}
	if sourceConfig == nil {
		return nil, nil, fmt.Errorf("source database config is required")
	}
	if strings.TrimSpace(sourceConfig.Database) == "" {
		cloned := *sourceConfig
		cloned.Database = resolvedSourceSchema
		sourceConfig = &cloned
	}
	if strings.TrimSpace(sourceConfig.Database) == "" {
		return nil, nil, fmt.Errorf("source schema is required")
	}

	targetConfig := task.Config.TargetDB
	if targetConfig == nil && s.config != nil && s.config.Target.Host != "" {
		targetConfig = &taskEntity.DatabaseConfig{
			Host:     s.config.Target.Host,
			Port:     s.config.Target.Port,
			Database: task.Config.TargetSchema,
			Username: s.config.Target.Username,
			Password: s.config.Target.Password,
		}
	}
	if targetConfig == nil {
		// 没有目标配置：借用源库连接信息，连接到目标 Schema
		targetSchema := task.Config.TargetSchema
		if targetSchema == "" {
			targetSchema = resolvedSourceSchema
		}
		targetConfig = &taskEntity.DatabaseConfig{
			Host:     sourceConfig.Host,
			Port:     sourceConfig.Port,
			Database: targetSchema,
			Username: sourceConfig.Username,
			Password: sourceConfig.Password,
		}
	}

	srcCompress := s.config != nil && s.config.Datasource.Compress
	// 只读对比使用较小连接池：单表源/目标各一个 COUNT(*) 查询，无需高并发。
	smallPool := &config.SyncTuneConfig{
		SourceMaxOpenConns:     4,
		SourceMaxIdleConns:     2,
		TargetMaxOpenConns:     4,
		TargetMaxIdleConns:     2,
		ConnMaxLifetimeSeconds: 120,
	}

	sourceDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		sourceConfig.Username, sourceConfig.Password,
		sourceConfig.Host, sourceConfig.Port, sourceConfig.Database,
		config.MySQLTCPParams(srcCompress))
	sourceDB, err = sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open source database for comparison: %w", err)
	}
	config.ApplySyncMySQLPool(sourceDB, smallPool, true, "row-count-compare-source")
	if err = sourceDB.Ping(); err != nil {
		sourceDB.Close()
		return nil, nil, fmt.Errorf("failed to ping source database for comparison: %w", err)
	}

	tgtCompress := s.config != nil && s.config.Target.Compress
	targetDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		targetConfig.Username, targetConfig.Password,
		targetConfig.Host, targetConfig.Port, targetConfig.Database,
		config.MySQLTCPParams(tgtCompress))
	targetDB, err = sql.Open("mysql", targetDSN)
	if err != nil {
		sourceDB.Close()
		return nil, nil, fmt.Errorf("failed to open target database for comparison: %w", err)
	}
	config.ApplySyncMySQLPool(targetDB, smallPool, false, "row-count-compare-target")
	if err = targetDB.Ping(); err != nil {
		sourceDB.Close()
		targetDB.Close()
		return nil, nil, fmt.Errorf("failed to ping target database for comparison: %w", err)
	}

	return sourceDB, targetDB, nil
}

// resolveComparisonTables 按现有同步规则解析待核对的源/目标表映射。
// 单库/多库/源库到目标库映射沿用 executeFullSync 的 dbPair 逻辑；
// 指定表任务只核对 Tables 配置范围，并沿用 TargetTables 重命名映射；
// 库级任务从源端 information_schema.TABLES 获取当前 BASE TABLE 列表（目标端独有表不纳入）。
func (s *TaskService) resolveComparisonTables(ctx context.Context, task *taskEntity.SyncTask, sourceDB *sql.DB) ([]comparisonTableTask, error) {
	type dbPair struct{ src, dst string }
	var pairs []dbPair
	if len(task.Config.SourceDatabases) > 0 {
		for i, src := range task.Config.SourceDatabases {
			dst := src
			if i < len(task.Config.TargetDatabases) && task.Config.TargetDatabases[i] != "" {
				dst = task.Config.TargetDatabases[i]
			}
			pairs = append(pairs, dbPair{src, dst})
		}
	} else {
		resolvedSourceSchema := s.resolveSourceSchema(task)
		if resolvedSourceSchema == "" {
			return nil, fmt.Errorf("source schema is required for single-database comparison")
		}
		dst := task.Config.TargetSchema
		if dst == "" {
			dst = resolvedSourceSchema
		}
		pairs = append(pairs, dbPair{resolvedSourceSchema, dst})
	}

	// 指定表模式：解析 Tables（支持 "schema.table" 限定）分组到各源库
	tablesBySource := make(map[string][]string)
	if len(task.Config.Tables) > 0 {
		defaultSource := task.Config.SourceSchema
		if defaultSource == "" && len(task.Config.SourceDatabases) > 0 {
			defaultSource = task.Config.SourceDatabases[0]
		} else if defaultSource == "" {
			defaultSource = s.resolveSourceSchema(task)
		}
		for _, fullTableName := range task.Config.Tables {
			sourceSchema := defaultSource
			tableName := fullTableName
			if parts := strings.SplitN(fullTableName, ".", 2); len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
			}
			if sourceSchema == "" || tableName == "" {
				continue
			}
			tablesBySource[sourceSchema] = append(tablesBySource[sourceSchema], tableName)
		}
	}

	var result []comparisonTableTask
	for _, p := range pairs {
		tables := tablesBySource[p.src]

		// 指定表模式但当前库无配置表时跳过
		if task.Config.SyncLevel == taskEntity.SyncLevelTable && len(task.Config.Tables) > 0 && len(tables) == 0 {
			continue
		}

		// 库级任务：从源端 information_schema 获取 BASE TABLE 列表
		if len(tables) == 0 {
			allTables, listErr := listBaseTables(ctx, sourceDB, p.src)
			if listErr != nil {
				return nil, fmt.Errorf("failed to list tables for source schema %s: %w", p.src, listErr)
			}
			tables = append(tables, allTables...)
		}

		for idx, tableName := range tables {
			targetTable := s.resolveTableTargetName(task, p.src, tableName, idx)
			result = append(result, comparisonTableTask{
				sourceSchema: p.src,
				sourceTable:  tableName,
				targetSchema: p.dst,
				targetTable:  targetTable,
			})
		}
	}

	return result, nil
}

// listBaseTables 直接从 information_schema.TABLES 读取 BASE TABLE 列表（只读，不依赖 analyzer）。
func listBaseTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	query := `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan tables for schema %s: %w", schema, err)
	}
	return names, nil
}

// countRowsExact 执行 SELECT COUNT(*) FROM `schema`.`table`，返回精确行数。
func countRowsExact(ctx context.Context, db *sql.DB, schema, table string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("database connection is nil")
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		quoteIdentifierForCount(schema), quoteIdentifierForCount(table))
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// rowAgg 是聚合过程中的中间统计值。
type rowAgg struct {
	checked, matched, mismatched, failed int
	sourceTotal, targetTotal             int64
}

func aggregateRowCountComparison(done []taskEntity.RowCountComparisonTable) rowAgg {
	var a rowAgg
	for _, t := range done {
		if t.SourceRows == nil || t.TargetRows == nil {
			a.failed++
			a.checked++
			continue
		}
		a.checked++
		a.sourceTotal += *t.SourceRows
		a.targetTotal += *t.TargetRows
		if *t.Difference == 0 {
			a.matched++
		} else {
			a.mismatched++
		}
	}
	return a
}

// deriveRowCountComparisonStatus 根据逐表结果推导最终汇总状态与失败原因（纯函数，便于测试）。
//
//	matched + mismatched = 两端均查询成功的表数。
//	没有任何一张表两端都成功（全部失败或表清单为空）-> FAILED
//	部分表失败（其余成功）-> PARTIAL
//	全部成功且至少一张不一致 -> MISMATCHED
//	全部成功且全部一致 -> MATCHED
func deriveRowCountComparisonStatus(tableResults []taskEntity.RowCountComparisonTable) (taskEntity.RowCountComparisonStatus, string) {
	agg := aggregateRowCountComparison(tableResults)
	successCount := agg.matched + agg.mismatched
	switch {
	case successCount == 0:
		if len(tableResults) == 0 {
			return taskEntity.RowCountComparisonFailed, "待核对表清单为空或连接失败"
		}
		return taskEntity.RowCountComparisonFailed, "所有表均无法完成核对"
	case agg.failed > 0:
		return taskEntity.RowCountComparisonPartial, ""
	case agg.mismatched > 0:
		return taskEntity.RowCountComparisonMismatched, ""
	default:
		return taskEntity.RowCountComparisonMatched, ""
	}
}

func countMatchedTables(done []taskEntity.RowCountComparisonTable) int {
	c := 0
	for _, t := range done {
		if t.SourceRows != nil && t.TargetRows != nil && t.Matched {
			c++
		}
	}
	return c
}

func countMismatchedTables(done []taskEntity.RowCountComparisonTable) int {
	c := 0
	for _, t := range done {
		if t.SourceRows != nil && t.TargetRows != nil && !t.Matched {
			c++
		}
	}
	return c
}

func countFailedTables(done []taskEntity.RowCountComparisonTable) int {
	c := 0
	for _, t := range done {
		if (t.SourceRows == nil || t.TargetRows == nil) && t.Error != "" {
			c++
		}
	}
	return c
}

// sumRows 求和：source=true 汇总源端成功行数，false 汇总目标端成功行数。
func sumRows(done []taskEntity.RowCountComparisonTable, source bool) int64 {
	var total int64
	for _, t := range done {
		if source && t.SourceRows != nil {
			total += *t.SourceRows
		}
		if !source && t.TargetRows != nil {
			total += *t.TargetRows
		}
	}
	return total
}

// persistRowCountComparisonProgress 写入核对进行中的快照（CHECKING + 已完成表）。
func (s *TaskService) persistRowCountComparisonProgress(taskID string, startedAt time.Time, allTables []comparisonTableTask, done []taskEntity.RowCountComparisonTable) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}
	agg := aggregateRowCountComparison(done)
	rc := &taskEntity.RowCountComparison{
		Status:           taskEntity.RowCountComparisonChecking,
		StartedAt:        &startedAt,
		TotalTables:      len(allTables),
		CheckedTables:    agg.checked,
		MatchedTables:    agg.matched,
		MismatchedTables: agg.mismatched,
		FailedTables:     agg.failed,
		SourceTotal:      agg.sourceTotal,
		TargetTotal:      agg.targetTotal,
		Difference:       agg.targetTotal - agg.sourceTotal,
		Tables:           append([]taskEntity.RowCountComparisonTable(nil), done...),
	}
	task.Context.RowCountComparison = rc
	task.Context.LastUpdateTime = time.Now()
	if err := s.storage.Save(task); err != nil {
		logger.Warn("[Task %s] failed to persist row-count comparison progress: %v", taskID, err)
	}
}

// finalizeRowCountComparison 写入核对最终状态并持久化。
func (s *TaskService) finalizeRowCountComparison(taskID string, startedAt time.Time, allTables []comparisonTableTask, tableResults []taskEntity.RowCountComparisonTable, fatalErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.tasks[taskID]
	if !exists {
		return
	}

	completedAt := time.Now()
	var status taskEntity.RowCountComparisonStatus
	var failureReason string

	if fatalErr != "" {
		status = taskEntity.RowCountComparisonFailed
		failureReason = fatalErr
	} else {
		status, failureReason = deriveRowCountComparisonStatus(tableResults)
	}

	srcTotal := sumRows(tableResults, true)
	tgtTotal := sumRows(tableResults, false)
	rc := &taskEntity.RowCountComparison{
		Status:           status,
		StartedAt:        &startedAt,
		CompletedAt:      &completedAt,
		TotalTables:      len(allTables),
		CheckedTables:    aggregateRowCountComparison(tableResults).checked,
		MatchedTables:    countMatchedTables(tableResults),
		MismatchedTables: countMismatchedTables(tableResults),
		FailedTables:     countFailedTables(tableResults),
		SourceTotal:      srcTotal,
		TargetTotal:      tgtTotal,
		Difference:       tgtTotal - srcTotal,
		Tables:           append([]taskEntity.RowCountComparisonTable(nil), tableResults...),
		FailureReason:    failureReason,
	}

	task.Context.RowCountComparison = rc
	task.Context.LastUpdateTime = time.Now()
	if err := s.storage.Save(task); err != nil {
		logger.Warn("[Task %s] failed to persist row-count comparison final state: %v", taskID, err)
	}
}

// runRowCountComparison 执行实际的逐表 COUNT(*) 核对。
//
// 表与表之间串行处理，避免对大库同时发起大量全表计数；
// 同一张表的源端和目标端计数并行执行，减少两端统计时间差。
//
// 每完成一张表即用不可变快照更新任务存档中的 RowCountComparison，供详情接口轮询。
// 单表计数失败不终止其他表，错误记录到对应表结果。
func (s *TaskService) runRowCountComparison(ctx context.Context, taskID string, task *taskEntity.SyncTask, preResolvedTables []comparisonTableTask) {
	startedAt := time.Now()

	// 创建只读连接（不持锁，避免长时间占用主锁）
	sourceDB, targetDB, err := s.resolveComparisonConnections(task)
	if err != nil {
		s.finalizeRowCountComparison(taskID, startedAt, preResolvedTables, nil, fmt.Sprintf("连接数据库失败: %v", err))
		return
	}
	defer func() {
		if sourceDB != nil {
			sourceDB.Close()
		}
		if targetDB != nil && targetDB != sourceDB {
			targetDB.Close()
		}
	}()

	// 解析待核对表清单（若调用方未预解析则按配置解析）
	tables := preResolvedTables
	if len(tables) == 0 {
		tables, err = s.resolveComparisonTables(ctx, task, sourceDB)
		if err != nil {
			s.finalizeRowCountComparison(taskID, startedAt, nil, nil, fmt.Sprintf("解析表清单失败: %v", err))
			return
		}
	}

	// 表清单就绪后再持久化 CHECKING，避免前端首轮轮询看到 0/0。
	s.mu.Lock()
	if task, exists := s.tasks[taskID]; exists {
		task.Context.RowCountComparison = &taskEntity.RowCountComparison{
			Status:      taskEntity.RowCountComparisonChecking,
			StartedAt:   &startedAt,
			TotalTables: len(tables),
		}
		task.Context.LastUpdateTime = time.Now()
		if err := s.storage.Save(task); err != nil {
			logger.Warn("[Task %s] failed to persist row-count comparison CHECKING state: %v", taskID, err)
		}
	} else {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if len(tables) == 0 {
		s.finalizeRowCountComparison(taskID, startedAt, tables, nil, "")
		return
	}

	// 逐表串行核对；同一张表源/目标并行
	tableResults := make([]taskEntity.RowCountComparisonTable, len(tables))
	for i, tbl := range tables {
		// 检查取消（任务删除/服务关闭）；取消路径不写最终状态，由 cancel 触发方处理。
		if ctx.Err() != nil {
			break
		}

		entry := taskEntity.RowCountComparisonTable{
			SourceSchema: tbl.sourceSchema,
			SourceTable:  tbl.sourceTable,
			TargetSchema: tbl.targetSchema,
			TargetTable:  tbl.targetTable,
		}

		var srcRows, tgtRows int64
		var srcErr, tgtErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			srcRows, srcErr = countRowsExact(ctx, sourceDB, tbl.sourceSchema, tbl.sourceTable)
		}()
		go func() {
			defer wg.Done()
			tgtRows, tgtErr = countRowsExact(ctx, targetDB, tbl.targetSchema, tbl.targetTable)
		}()
		wg.Wait()

		if srcErr == nil {
			entry.SourceRows = &srcRows
		} else {
			entry.Error = "source: " + srcErr.Error()
		}
		if tgtErr == nil {
			entry.TargetRows = &tgtRows
		} else {
			if entry.Error != "" {
				entry.Error += "; "
			}
			entry.Error += "target: " + tgtErr.Error()
		}
		if srcErr == nil && tgtErr == nil {
			diff := tgtRows - srcRows
			entry.Difference = &diff
			entry.Matched = diff == 0
		}
		tableResults[i] = entry

		// 每完成一张表即持久化不可变快照，供详情接口轮询
		s.persistRowCountComparisonProgress(taskID, startedAt, tables, tableResults[:i+1])
	}

	// 取消导致的退出：不写最终状态（由 cancel 触发方决定），避免覆盖删除流程。
	if ctx.Err() != nil {
		return
	}

	s.finalizeRowCountComparison(taskID, startedAt, tables, tableResults, "")
}

// StartRowCountComparison 校验并启动后台行数核对。成功返回 nil 表示已启动（接口应返回 202）。
//
// 允许条件：状态为 COMPLETED 或 STOPPED；模式为 FULL 或 ALL；
// HasFullSyncEverCompleted() 为真；目标端为 MySQL（当前实现即 MySQL）。
// 同一任务已存在 CHECKING 核对时返回冲突错误（接口应返回 409）。
func (s *TaskService) StartRowCountComparison(taskID string) error {
	s.mu.Lock()
	task, exists := s.tasks[taskID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 校验状态
	switch task.Context.Status {
	case taskEntity.TaskStatusCompleted, taskEntity.TaskStatusStopped:
		// 允许
	default:
		s.mu.Unlock()
		return fmt.Errorf("task is not in a comparable status (status=%s): %s", task.Context.Status, taskID)
	}

	// 校验模式
	switch task.Config.Mode {
	case taskEntity.SyncModeFull, taskEntity.SyncModeAll:
		// 允许
	default:
		s.mu.Unlock()
		return fmt.Errorf("task mode does not support row count comparison (mode=%s): %s", task.Config.Mode, taskID)
	}

	// 校验全量已完成
	if !task.HasFullSyncEverCompleted() {
		s.mu.Unlock()
		return fmt.Errorf("full sync has not completed yet, cannot compare row counts: %s", taskID)
	}

	// 目标端为 MySQL：当前实现仅支持 MySQL 目标。SinkConfigs 非 MySQL 视为非 MySQL 目标。
	if !taskTargetIsMySQL(task) {
		s.mu.Unlock()
		return fmt.Errorf("row count comparison requires MySQL target: %s", taskID)
	}

	// 重复核对拦截（存档状态）
	if task.Context.RowCountComparison != nil && task.Context.RowCountComparison.Status == taskEntity.RowCountComparisonChecking {
		s.mu.Unlock()
		return fmt.Errorf("row count comparison already in progress: %s", taskID)
	}

	// 注册 cancel + wait group（comparisonMu 保护，避免与 Delete/Close 竞争）
	s.comparisonMu.Lock()
	if _, running := s.comparisonCancels[taskID]; running {
		s.comparisonMu.Unlock()
		s.mu.Unlock()
		return fmt.Errorf("row count comparison already in progress: %s", taskID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	s.comparisonCancels[taskID] = cancel
	s.comparisonWgs[taskID] = wg
	s.comparisonMu.Unlock()

	// 取任务副本用于连接解析（goroutine 内不长时间持锁）
	taskCopy := *task
	s.mu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			s.comparisonMu.Lock()
			delete(s.comparisonCancels, taskID)
			if cur, ok := s.comparisonWgs[taskID]; ok && cur == wg {
				delete(s.comparisonWgs, taskID)
			}
			s.comparisonMu.Unlock()
			cancel()
		}()
		defer func() {
			if r := recover(); r != nil {
				logger.Error("[Task %s] row count comparison panicked: %v", taskID, r)
				s.finalizeRowCountComparison(taskID, time.Now(), nil, nil, fmt.Sprintf("核对异常崩溃: %v", r))
			}
		}()

		logger.Info("[Task %s] row count comparison started", taskID)
		s.runRowCountComparison(ctx, taskID, &taskCopy, nil)
	}()

	return nil
}

// cancelRowComparisonAndWait 取消并等待某任务的行数核对 goroutine 退出。
// 调用方不得持有 s.mu（goroutine 内部需要获取 s.mu 持久化结果）。
func (s *TaskService) cancelRowComparisonAndWait(taskID string) {
	s.comparisonMu.Lock()
	cancel, ok := s.comparisonCancels[taskID]
	wg := s.comparisonWgs[taskID]
	s.comparisonMu.Unlock()
	if !ok || cancel == nil {
		return
	}
	cancel()
	if wg != nil {
		wg.Wait()
	}
}

// taskTargetIsMySQL 判断任务目标端是否为 MySQL。
// 无 SinkConfigs 时默认 MySQL；存在 SinkConfigs 时要求全部为 MYSQL 类型。
func taskTargetIsMySQL(task *taskEntity.SyncTask) bool {
	if task == nil {
		return false
	}
	if len(task.Config.SinkConfigs) == 0 {
		return true
	}
	for _, sc := range task.Config.SinkConfigs {
		if sc.Type != sink.SinkTypeMYSQL {
			return false
		}
	}
	return true
}
