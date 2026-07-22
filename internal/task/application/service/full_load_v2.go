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

	"github.com/go-mysql-org/go-mysql/mysql"
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
// 每表数据全部提交后立即异步重建该表索引（P2），不再攒到任务末尾统一阶段3。
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

	readyByKey := make(map[string]tableReadyV2, len(ready))
	specs := make([]*fullload.TableSpec, 0, len(ready))
	for _, r := range ready {
		s.startTableProgress(taskID, sourceSchema, r.sourceName, r.estimated)
		readyByKey[sourceSchema+"."+r.sourceName] = r
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

	engineCtx, engineCancel := context.WithCancel(ctx)
	defer engineCancel()

	var (
		restoreWG      sync.WaitGroup
		restoreErrOnce sync.Once
		restoreErr     error
	)
	hardMax := 0
	if s.config != nil {
		hardMax = s.config.Sync.IndexRestoreHardMax
	}
	indexWorkers := taskEntity.EffectiveIndexRestoreWorkers(
		task.Config.IndexRestoreWorkerCount,
		task.Config.WorkerCount,
		hardMax,
	)
	restoreSem := make(chan struct{}, indexWorkers)

	setRestoreErr := func(err error) {
		if err == nil {
			return
		}
		restoreErrOnce.Do(func() {
			restoreErr = err
			engineCancel()
		})
	}

	engine := &fullload.Engine{
		SourceDB:        runtime.sourceDB,
		TargetDB:        runtime.targetDB,
		Options:         opt,
		Stats:           stats,
		TaskID:          taskID,
		IsStopped:       func() bool { return s.isTaskStopped(taskID) },
		CaptureTableHWM: task.Config.Mode == taskEntity.SyncModeAll,
		OnTableSnapshotReady: func(schema, table string, pos mysql.Position) error {
			if err := s.persistTableBinlogHWM(taskID, schema, table, pos); err != nil {
				return err
			}
			logger.Info("[Task %s] FullLoadV2: persisted table binlog HWM for %s.%s at %s:%d",
				taskID, schema, table, pos.Name, pos.Pos)
			return nil
		},
		OnCommit: func(schema, table string, rows, bytes int64) {
			mark := schema + "." + table
			taskTotalRows := s.incrementTaskProgress(taskID, rows, mark)
			elapsed := time.Since(taskStartTime).Seconds()
			s.updateTableProgress(taskID, schema, table, rows, elapsed, taskStartTime, taskTotalRows)
		},
		OnTableDataReady: func(schema, table string) error {
			key := schema + "." + table
			r, ok := readyByKey[key]
			if !ok {
				return fmt.Errorf("unknown table ready callback for %s", key)
			}
			s.completeTableProgress(taskID, schema, table)

			if !task.Config.OptimizeIndex || len(r.savedIndexes) == 0 {
				return nil
			}
			// 异步重建索引，不阻塞写入路径；失败则取消引擎。
			indexes := append([]map[string]interface{}(nil), r.savedIndexes...)
			targetSchemaName := targetSchema
			targetTableName := r.targetName
			restoreWG.Add(1)
			go func() {
				defer restoreWG.Done()
				select {
				case restoreSem <- struct{}{}:
					defer func() { <-restoreSem }()
				case <-engineCtx.Done():
					return
				}
				if s.isTaskStopped(taskID) || engineCtx.Err() != nil {
					return
				}
				logger.Info("[Task %s] FullLoadV2: restoring indexes for %s.%s (per-table)", taskID, targetSchemaName, targetTableName)
				if err := s.restoreIndexes(engineCtx, runtime, targetSchemaName, targetTableName, indexes); err != nil {
					setRestoreErr(fmt.Errorf("restore indexes for %s.%s: %w", targetSchemaName, targetTableName, err))
					return
				}
				logger.Info("[Task %s] FullLoadV2: restored indexes for %s.%s", taskID, targetSchemaName, targetTableName)
			}()
			return nil
		},
	}

	runErr := engine.Run(engineCtx, specs)
	restoreWG.Wait()

	if s.isTaskStopped(taskID) {
		logger.Info("[Task %s] FullLoadV2 stopped during pair %s->%s: %v", taskID, sourceSchema, targetSchema, runErr)
		return errFullSyncStoppedByUser
	}
	if restoreErr != nil {
		s.failTaskUnlessCancelled(ctx, taskID, restoreErr.Error())
		return restoreErr
	}
	if runErr != nil {
		s.failTaskUnlessCancelled(ctx, taskID, runErr.Error())
		return runErr
	}

	// 逐表索引已在 OnTableDataReady 中处理；不再写入外层 pending（避免阶段3重复重建）。
	_ = pending
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
