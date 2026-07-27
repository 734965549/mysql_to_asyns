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

	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup

	mu         sync.Mutex
	firstErr   error
	attemptIDs map[string]int
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
		workerCtx:    wctx,
		workerCancel: wc,
		attemptIDs:   make(map[string]int),
	}
}

func (c *readCoordinator) startWorkers(n int) {
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
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
		c.mu.Unlock()

		if err := c.budget.Acquire(c.workerCtx, tableKey, perTable); err != nil {
			return
		}
		if c.stats != nil {
			c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
		}
		err := readChunk(c.workerCtx, c.db, chunk, c.q, c.opt, c.stats, c.tracker, c.stateTracker, c.isStopped, c.taskCancel, attemptID, c.sink)
		c.budget.Release(tableKey)
		if c.stats != nil {
			c.stats.setReadBudgetInUse(int64(c.budget.InUse()))
		}
		c.scheduler.markDone(schema, table)

		if err != nil {
			c.setErr(err)
			return
		}
	}
}

func (c *readCoordinator) setErr(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.firstErr = preferReaderError(c.firstErr, err)
	c.mu.Unlock()
	c.workerCancel()
}

// submitTable 将表 chunk 注册到公平调度器并等待全部完成。
func (c *readCoordinator) submitTable(ctx context.Context, schema, table string, chunks []*Chunk, attemptID int) error {
	if len(chunks) == 0 {
		if c.tracker != nil {
			return c.tracker.markReadDone(schema, table)
		}
		return nil
	}
	tableKey := tableKey(schema, table)
	c.mu.Lock()
	c.attemptIDs[tableKey] = attemptID
	c.mu.Unlock()

	c.scheduler.addTable(schema, table, chunks)

	for c.scheduler.tablePending(tableKey) > 0 {
		c.mu.Lock()
		err := c.firstErr
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.workerCtx.Err() != nil {
			c.mu.Lock()
			err := c.firstErr
			c.mu.Unlock()
			if err != nil {
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
