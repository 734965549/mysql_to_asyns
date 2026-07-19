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
// 结构与 V1 syncDatabasePair 保持一致：阶段1 串行准备表结构（复用 ensureTargetTable /
// dropNonPrimaryKeyIndexes），阶段2 由 fullload.Engine 以任务级 chunk 调度 + 读写解耦
// 流水线复制数据，索引恢复任务写入 pending 由最外层统一执行。
func (s *TaskService) syncDatabasePairV2(ctx context.Context, task *taskEntity.SyncTask, runtime *taskRuntime, sourceSchema, targetSchema string, specifiedTables []string, pending *[]pendingIndexRestore, dbLevelRebuilt bool) error {
	taskID := task.Config.ID

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
	logger.Info("[Task %s] FullLoadV2 阶段1: 准备 %d 个表结构...", taskID, len(tables))
	ready := make([]tableReadyV2, 0, len(tables))
	for i, tableName := range tables {
		if s.isTaskStopped(taskID) {
			return errFullSyncStoppedByUser
		}
		targetTableName := s.resolveTableTargetName(task, sourceSchema, tableName, i)
		identity, err := runtime.analyzer.AnalyzeTable(sourceSchema, tableName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to analyze table %s: %v", tableName, err)
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}

		effectiveDropBeforeDDL := task.Config.EnableDropTableBeforeDDL && !dbLevelRebuilt
		savedIndexes, err := s.ensureTargetTable(runtime, sourceSchema, targetSchema, tableName, targetTableName, task.Config.OptimizeIndex, effectiveDropBeforeDDL)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to ensure target table %s.%s -> %s.%s: %v", sourceSchema, tableName, targetSchema, targetTableName, err)
			s.failTaskUnlessCancelled(ctx, taskID, errMsg)
			return fmt.Errorf("%s", errMsg)
		}
		if task.Config.OptimizeIndex && len(savedIndexes) == 0 {
			indexes, dropErr := s.dropNonPrimaryKeyIndexes(runtime, targetSchema, targetTableName)
			if dropErr != nil {
				logger.Warn("[Task %s] FullLoadV2: drop indexes for %s.%s failed: %v", taskID, targetSchema, targetTableName, dropErr)
			} else {
				savedIndexes = indexes
			}
		}

		est, _ := reader.NewReader(runtime.sourceDB, sourceSchema, tableName, identity).GetEstimatedCount(ctx)
		ready = append(ready, tableReadyV2{sourceName: tableName, targetName: targetTableName, identity: identity, savedIndexes: savedIndexes, estimated: est})
	}
	logger.Info("[Task %s] FullLoadV2 阶段1完成：%d 个表就绪", taskID, len(ready))

	if len(ready) == 0 {
		return nil
	}

	// 构建引擎输入并标记表进度开始。
	specs := make([]*fullload.TableSpec, 0, len(ready))
	for _, r := range ready {
		s.startTableProgress(taskID, sourceSchema, r.sourceName, r.estimated)
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
	})

	stats := &fullload.Stats{}
	setFullLoadStats(taskID, stats)

	taskStartTime := s.getTaskStartTime(taskID)
	if taskStartTime.IsZero() {
		taskStartTime = time.Now()
	}

	engine := &fullload.Engine{
		SourceDB:  runtime.sourceDB,
		TargetDB:  runtime.targetDB,
		Options:   opt,
		Stats:     stats,
		TaskID:    taskID,
		IsStopped: func() bool { return s.isTaskStopped(taskID) },
		OnCommit: func(schema, table string, rows, bytes int64) {
			mark := schema + "." + table
			taskTotalRows := s.incrementTaskProgress(taskID, rows, mark)
			elapsed := time.Since(taskStartTime).Seconds()
			s.updateTableProgress(taskID, schema, table, rows, elapsed, taskStartTime, taskTotalRows)
		},
	}

	if err := engine.Run(ctx, specs); err != nil {
		if s.isTaskStopped(taskID) {
			logger.Info("[Task %s] FullLoadV2 stopped during pair %s->%s: %v", taskID, sourceSchema, targetSchema, err)
			return errFullSyncStoppedByUser
		}
		s.failTaskUnlessCancelled(ctx, taskID, err.Error())
		return err
	}

	if s.isTaskStopped(taskID) {
		return errFullSyncStoppedByUser
	}

	// 数据复制完成：标记表进度完成，索引恢复任务入队。
	for _, r := range ready {
		s.completeTableProgress(taskID, sourceSchema, r.sourceName)
	}
	if pending != nil && task.Config.OptimizeIndex {
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
