package fullload

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"
)

// readCoordinator 协调全局读取预算、公平 chunk 调度与 chunk worker。
type readCoordinator struct {
	scheduler *chunkScheduler
	budget    *ReadBudget
	opt       Options

	db           *sql.DB
	q            *batchQueue
	stats        *Stats
	tracker      *tableCompletionTracker
	stateTracker *tableStateTracker
	sink         EventSink
	isStopped    func() bool
	taskCancel   context.CancelFunc

	parentCtx    context.Context
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	workerCount  int
	activeWorkers atomic.Int32

	mu             sync.Mutex
	firstErr       error
	tableErrors    map[string]error
	attemptIDs     map[string]int
	attemptCtxs    map[string]context.Context
	attemptCancels map[string]context.CancelFunc
}

func newReadCoordinator(
	ctx context.Context,
	db *sql.DB,
	q *batchQueue,
	opt Options,
	stats *Stats,
	tracker *tableCompletionTracker,
	stateTracker *tableStateTracker,
	sink EventSink,
	isStopped func() bool,
	taskCancel context.CancelFunc,
) *readCoordinator {
	wctx, wc := context.WithCancel(ctx)
	return &readCoordinator{
		scheduler:    newChunkScheduler(),
		budget:       NewReadBudget(opt.GlobalReadBudget),
		opt:          opt,
		db:           db,
		q:            q,
		stats:        stats,
		tracker:      tracker,
		stateTracker: stateTracker,
		sink:         sink,
		isStopped:    isStopped,
		taskCancel:   taskCancel,
		parentCtx:    ctx,
		workerCtx:    wctx,
		workerCancel: wc,
		attemptIDs:     make(map[string]int),
		attemptCtxs:    make(map[string]context.Context),
		attemptCancels: make(map[string]context.CancelFunc),
		tableErrors:    make(map[string]error),
	}
}

func (c *readCoordinator) startWorkers(n int) {
	if n < 1 {
		n = 1
	}
	c.workerCount = n
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.activeWorkers.Add(1)
			defer c.activeWorkers.Add(-1)
			atomic.AddInt64(&c.stats.ActiveReaders, 1)
			defer atomic.AddInt64(&c.stats.ActiveReaders, -1)
			c.workerLoop()
		}()
	}
}

func (c *readCoordinator) workerLoop() {
	for {
		if c.workerCtx.Err() != nil {
			return
		}
		waiting := c.scheduler.waitingTables()
		perTable := PerTableEffectiveLimit(c.opt.TableParallelReaders, c.opt.GlobalReadBudget, waiting)
		chunk, ok := c.scheduler.next(c.workerCtx, perTable)
		if !ok {
			return
		}
		if chunk == nil || chunk.Spec == nil {
			continue
		}
		schema := chunk.Spec.SourceSchema
		table := chunk.Spec.SourceTable
		tableKey := tableKey(schema, table)

		c.mu.Lock()
		attemptID := c.attemptIDs[tableKey]
		chunkCtx := c.attemptCtxs[tableKey]
		c.mu.Unlock()
		if chunkCtx == nil {
			chunkCtx = c.workerCtx
		}

		if err := c.budget.Acquire(chunkCtx, tableKey, perTable); err != nil {
			c.scheduler.markDone(schema, table)
			if c.workerCtx.Err() != nil {
				return
			}
			continue
		}
		if c.stats != nil {
			c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
		}
		err := readChunk(chunkCtx, c.db, chunk, c.q, c.opt, c.stats, c.tracker, c.stateTracker, c.isStopped, c.taskCancel, attemptID, c.sink)
		c.budget.Release(tableKey)
		if c.stats != nil {
			c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
		}
		c.scheduler.markDone(schema, table)

		if err != nil {
			if c.setTableErr(schema, table, attemptID, err) {
				c.cancelTableAttempt(schema, table, attemptID)
				c.markTableFailed(schema, table, attemptID)
			}
			continue
		}
	}
}

func (c *readCoordinator) setTableErr(schema, table string, attemptID int, err error) bool {
	if err == nil {
		return false
	}
	key := tableKey(schema, table)
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.attemptIDs[key]; ok && current != attemptID {
		return false
	}
	c.tableErrors[key] = preferReaderError(c.tableErrors[key], err)
	c.firstErr = preferReaderError(c.firstErr, err)
	return true
}

func (c *readCoordinator) cancelTableAttempt(schema, table string, attemptID int) {
	if c == nil {
		return
	}
	key := tableKey(schema, table)
	c.mu.Lock()
	current, ok := c.attemptIDs[key]
	cancel := c.attemptCancels[key]
	c.mu.Unlock()
	if !ok || current != attemptID || cancel == nil {
		return
	}
	cancel()
}

func (c *readCoordinator) cancelCurrentTableAttempt(schema, table string) {
	if c == nil {
		return
	}
	key := tableKey(schema, table)
	c.mu.Lock()
	cancel := c.attemptCancels[key]
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *readCoordinator) markTableFailed(schema, table string, attemptID int) {
	if c == nil || c.scheduler == nil {
		return
	}
	key := tableKey(schema, table)
	c.mu.Lock()
	current := c.attemptIDs[key]
	c.mu.Unlock()
	if current != attemptID {
		return
	}
	c.scheduler.markTableFailed(schema, table)
}

func (c *readCoordinator) tableErr(schema, table string) error {
	key := tableKey(schema, table)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tableErrors[key]
}

// prepareTableRetry 在表级重试前清理失败状态并恢复 worker（若已退出）。
func (c *readCoordinator) prepareTableRetry(schema, table string) {
	if c == nil {
		return
	}
	c.scheduler.removeTable(schema, table)
	tableKey := tableKey(schema, table)
	c.cancelCurrentTableAttempt(schema, table)
	c.mu.Lock()
	delete(c.attemptCtxs, tableKey)
	delete(c.attemptCancels, tableKey)
	delete(c.tableErrors, tableKey)
	c.firstErr = nil
	for k := range c.tableErrors {
		if c.tableErrors[k] != nil {
			c.firstErr = preferReaderError(c.firstErr, c.tableErrors[k])
		}
	}
	c.mu.Unlock()
	if c.workerCtx.Err() != nil {
		c.wg.Wait()
		wctx, wc := context.WithCancel(c.parentCtx)
		c.workerCtx = wctx
		c.workerCancel = wc
		c.startWorkers(c.workerCount)
	}
}

// acquirePlanBudget 为 chunk 规划占用全局读取预算（与扫描共用同一令牌池）。
func (c *readCoordinator) acquirePlanBudget(ctx context.Context, schema, table string, readers int) error {
	if c == nil || c.budget == nil {
		return nil
	}
	key := tableKey(schema, table)
	waiting := c.scheduler.waitingTables() + 1
	perTable := PerTableEffectiveLimit(readers, c.opt.GlobalReadBudget, waiting)
	if err := c.budget.Acquire(ctx, key, perTable); err != nil {
		return err
	}
	if c.stats != nil {
		c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
	}
	return nil
}

func (c *readCoordinator) releasePlanBudget(schema, table string) {
	if c == nil || c.budget == nil {
		return
	}
	c.budget.Release(tableKey(schema, table))
	if c.stats != nil {
		c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
	}
}

// submitTable 将表 chunk 注册到公平调度器并等待全部完成。
func (c *readCoordinator) submitTable(ctx context.Context, schema, table string, chunks []*Chunk, attemptID int, attemptCancel context.CancelFunc) error {
	if len(chunks) == 0 {
		if c.tracker != nil {
			return c.tracker.markReadDone(schema, table)
		}
		return nil
	}
	tableKey := tableKey(schema, table)
	c.mu.Lock()
	c.attemptIDs[tableKey] = attemptID
	c.attemptCtxs[tableKey] = ctx
	c.attemptCancels[tableKey] = attemptCancel
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.attemptCtxs, tableKey)
		delete(c.attemptCancels, tableKey)
		c.mu.Unlock()
	}()

	c.scheduler.addTable(schema, table, chunks)

	for c.scheduler.tablePending(tableKey) > 0 {
		if err := c.tableErr(schema, table); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.workerCtx.Err() != nil {
			if err := c.tableErr(schema, table); err != nil {
				return err
			}
			return c.workerCtx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	if c.tracker != nil {
		return c.tracker.markReadDone(schema, table)
	}
	return nil
}

func (c *readCoordinator) finish() error {
	c.scheduler.close()
	c.workerCancel()
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.firstErr
}

func (c *readCoordinator) budgetInUse() int {
	if c.budget == nil {
		return 0
	}
	return c.budget.InUse()
}
