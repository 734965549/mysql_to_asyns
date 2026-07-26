package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mysql-to-sync/internal/metadata/domain/entity"
	"mysql-to-sync/internal/sync/fullload"
	"mysql-to-sync/internal/sync/infrastructure/reader"
	taskEntity "mysql-to-sync/internal/task/domain/entity"
	"mysql-to-sync/pkg/logger"
)

// fullLoadStatsStore 保存每个任务最近一次 V2 引擎运行的统计快照指针，供任务详情接口读取。
// 使用包级 map 避免改动庞大的 TaskService 结构体；键为 taskID。
var (
	fullLoadStatsStore = struct {
		sync.RWMutex
		m map[string]*fullload.Stats
	}{m: make(map[string]*fullload.Stats)}
)

func setFullLoadStats(taskID string, stats *fullload.Stats) {
	fullLoadStatsStore.Lock()
	fullLoadStatsStore.m[taskID] = stats
	fullLoadStatsStore.Unlock()
}

func clearFullLoadStats(taskID string) {
	fullLoadStatsStore.Lock()
	delete(fullLoadStatsStore.m, taskID)
	fullLoadStatsStore.Unlock()
}

// fullLoadStatsSnapshot 返回任务最近一次 V2 运行的统计快照；无数据时返回 (nil,false)。
func fullLoadStatsSnapshot(taskID string) (fullload.StatsSnapshot, bool) {
	fullLoadStatsStore.RLock()
	st := fullLoadStatsStore.m[taskID]
	fullLoadStatsStore.RUnlock()
	if st == nil {
		return fullload.StatsSnapshot{}, false
	}
	return st.Snapshot(), true
}

type tableReadyV2 struct {
	sourceName   string
	targetName   string
	identity     *entity.TableIdentity
	savedIndexes []map[string]interface{}
	estimated    int64
}

// syncDatabasePairV2 使用全量 V2 任务级流水线引擎同步单个源库到目标库。
//
// 阶段1 串行准备表结构；阶段2 表级一致性快照 + 任务级读写流水线；
// 索引恢复写入 pending，由最外层在全部表数据完成后统一阶段3执行。
// 禁止与灌数并行建索引：两者共用 runtime.targetDB 连接池，重叠会打满连接并拖垮目标库。
//
// schemaLocks 由调用层预先获取并传入，覆盖完整任务生命周期（含 DDL 与索引恢复），
// Engine 内部不再自行获取/释放锁。
func (s *TaskService) syncDatabasePairV2(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, sourceSchema, targetSchema string, specifiedTables []string, pending *[]pendingIndexRestore, dbLevelRebuilt bool, schemaLocks *fullload.SchemaLocks) error {
	taskID := task.Config.ID

	if err := task.Config.ValidateFullLoadOptions(); err != nil {
		errMsg := fmt.Sprintf("invalid full-load options: %v", err)
		s.failTaskUnlessCancelled(ctx, taskID, errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	tables := append([]string{}, specifiedTables...)
	if len(tables) == 0 {
		allTables, err := runtime.analyzer.GetAllTables(sourceSchema)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get tables for database %s: %v", sourceSchema, err)
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		for _, t := range allTables {
			tables = append(tables, t.TableName)
		}
	}

	// === 阶段1：串行准备表结构（集中 DDL）===
	// 恢复模式：必须在任何 destructive DDL 之前识别并跳过已 PUBLISHED 的表。
	logger.Info("[Task %s] FullLoadV2 阶段1: 准备 %d 个表结构...", taskID, len(tables))
	ready := make([]tableReadyV2, 0, len(tables))
	skippedPublished := 0
	for i, tableName := range tables {
		if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
			return err
		}
		tableKey := sourceSchema + "." + tableName
		if st := task.GetFullLoadV2TableState(tableKey); st != nil && st.Phase == "PUBLISHED" {
			skippedPublished++
			logger.Info("[Task %s] FullLoadV2: skip already-published table %s (before DDL)", taskID, tableKey)
			continue
		}

		targetTableName := s.resolveTableTargetName(task, sourceSchema, tableName, i)
		identity, err := runtime.analyzer.AnalyzeTable(sourceSchema, tableName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to analyze table %s: %v", tableName, err)
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}

		effectiveDropBeforeDDL := task.Config.EnableDropTableBeforeDDL && !dbLevelRebuilt
		savedIndexes, err := s.ensureTargetTable(ctx, runtime, sourceSchema, targetSchema, tableName, targetTableName, task.Config.OptimizeIndex, effectiveDropBeforeDDL, identity, task.Config.Mode)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to ensure target table %s.%s -> %s.%s: %v", sourceSchema, tableName, targetSchema, targetTableName, err)
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		needDefer := (task.Config.OptimizeIndex || task.Config.Mode == taskEntity.SyncModeAll) && len(savedIndexes) == 0
		if needDefer {
			indexes, dropErr := s.dropDeferredIndexes(ctx, runtime, targetSchema, targetTableName, identity, task.Config.Mode, task.Config.OptimizeIndex)
			if dropErr != nil {
				logger.Warn("[Task %s] FullLoadV2: drop deferred indexes for %s.%s failed: %v", taskID, targetSchema, targetTableName, dropErr)
			} else {
				savedIndexes = indexes
			}
		}

		est, _ := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity).GetEstimatedCount(ctx)
		ready = append(ready, tableReadyV2{sourceName: tableName, targetName: targetTableName, identity: identity, savedIndexes: savedIndexes, estimated: est})
	}
	logger.Info("[Task %s] FullLoadV2 阶段1完成：%d 个表就绪 (skipped_published=%d)", taskID, len(ready), skippedPublished)

	if len(ready) == 0 {
		return nil
	}

	readyByKey := make(map[string]tableReadyV2, len(ready))
	specs := make([]*fullload.TableSpec, 0, len(ready))
	for _, r := range ready {
		tableKey := sourceSchema + "." + r.sourceName
		s.startTableProgress(taskID, sourceSchema, r.sourceName, r.estimated)
		readyByKey[tableKey] = r
		specs = append(specs, &fullload.TableSpec{
			SourceSchema:  sourceSchema,
			TargetSchema:  targetSchema,
			SourceTable:   r.sourceName,
			TargetTable:   r.targetName,
			Identity:      r.identity,
			EstimatedRows: r.estimated,
		})
	}

	opt := fullload.ResolveOptions(fullload.RawOptions{
		ReadWorkers:                  task.Config.FullLoadReadWorkers,
		WriteWorkers:                 task.Config.FullLoadWriteWorkers,
		BufferMB:                     task.Config.FullLoadBufferMB,
		BatchBytesMB:                 task.Config.FullLoadBatchBytesMB,
		CommitRows:                   task.Config.FullLoadCommitRows,
		CommitBytesMB:                task.Config.FullLoadCommitBytesMB,
		BatchSize:                    task.Config.BatchSize,
		LegacyTxCommitEveryNParallel: task.Config.TxCommitEveryNParallel,
		SkipBinlog:                   task.Config.EnableSkipBinlog,
		QueryTimeoutSec:              task.Config.FullLoadQueryTimeoutSec,
		StreamIdleTimeoutSec:         task.Config.FullLoadStreamIdleTimeoutSec,
		StreamMaxDurationSec:         task.Config.FullLoadStreamMaxDurationSec,
		SlowQueryWarnSec:             task.Config.FullLoadSlowQueryWarnSec,
		TableNoProgressSec:           task.Config.FullLoadTableNoProgressSec,
		ReadRetryTimes:               task.Config.FullLoadReadRetryTimes,
		EnableTwoPhaseRead:           task.Config.FullLoadTwoPhaseRead,
		EnableStaging:                task.Config.FullLoadEnableStaging,
	})
	if err := opt.Validate(); err != nil {
		errMsg := fmt.Sprintf("invalid full-load options: %v", err)
		s.failTaskUnlessCancelled(ctx, taskID, errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	stats := &fullload.Stats{}
	setFullLoadStats(taskID, stats)

	taskStartTime := s.getTaskStartTime(taskID)
	if taskStartTime.IsZero() {
		taskStartTime = time.Now()
	}

	engine := &fullload.Engine{
		SourceDB:    runtime.sourceDB,
		TargetDB:    runtime.targetDB,
		Options:     opt,
		Stats:       stats,
		TaskID:      taskID,
		SchemaLocks: schemaLocks,
		IsStopped:   func() bool { return s.isTaskStopped(taskID) },
		OnCommit: func(schema, table string, rows, bytes int64) {
			mark := schema + "." + table
			taskTotalRows := s.incrementTaskProgress(taskID, rows, mark)
			elapsed := time.Since(taskStartTime).Seconds()
			s.updateTableProgress(taskID, schema, table, rows, elapsed, taskStartTime, taskTotalRows)
		},
		OnTableDataReady: func(schema, table string) error {
			if _, ok := readyByKey[schema+"."+table]; !ok {
				return fmt.Errorf("unknown table ready callback for %s.%s", schema, table)
			}
			s.completeTableProgress(taskID, schema, table)
			return nil
		},
		OnTableStateChange: func(schema, table, phase string, attemptID int, stagingTable, errMsg string, committedRows int64) error {
			return s.persistFullLoadV2TableState(taskID, schema, table, phase, attemptID, stagingTable, errMsg, committedRows)
		},
	}

	runErr := engine.Run(ctx, specs)
	if err := s.abortFullSyncIfCancelled(ctx, taskID); err != nil {
		logger.Info("[Task %s] FullLoadV2 stopped during pair %s->%s: %v", taskID, sourceSchema, targetSchema, runErr)
		return err
	}
	if runErr != nil {
		s.failTaskUnlessCancelled(ctx, taskID, runErr.Error())
		return runErr
	}

	// 数据复制完成后再入队索引恢复，避免与写路径争抢目标库连接池。
	if pending != nil {
		for _, r := range ready {
			if len(r.savedIndexes) == 0 {
				continue
			}
			*pending = append(*pending, pendingIndexRestore{
				targetSchema: targetSchema,
				targetTable:  r.targetName,
				indexes:      append([]map[string]interface{}(nil), r.savedIndexes...),
			})
		}
	}
	return nil
}

func (s *TaskService) getTaskStartTime(taskID string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if task := s.tasks[taskID]; task != nil {
		return task.Context.StartTime
	}
	return time.Time{}
}
