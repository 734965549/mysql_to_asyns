package fullload

import (
	"context"
	"sync"
	"time"
)

// chunkScheduler 公平派发 chunk：多表等待时每轮每表最多 1 个；仅剩单表时可连续派发。
type chunkScheduler struct {
	mu sync.Mutex

	tables         []*tableChunkQueue
	closed         bool
	activePerTable map[string]int
	rrIndex        int
}

type tableChunkQueue struct {
	key    string
	schema string
	table  string
	chunks []*Chunk
	next   int
}

func newChunkScheduler() *chunkScheduler {
	return &chunkScheduler{
		activePerTable: make(map[string]int),
	}
}

func (s *chunkScheduler) addTable(schema, table string, chunks []*Chunk) {
	if s == nil {
		return
	}
	key := tableKey(schema, table)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.tables = append(s.tables, &tableChunkQueue{
		key:    key,
		schema: schema,
		table:  table,
		chunks: chunks,
	})
}

func (s *chunkScheduler) waitingTables() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, tq := range s.tables {
		if tq != nil && tq.next < len(tq.chunks) {
			n++
		}
	}
	return n
}

func (s *chunkScheduler) next(ctx context.Context, perTableLimit int) (*Chunk, bool) {
	if s == nil {
		return nil, false
	}
	if perTableLimit < 1 {
		perTableLimit = 1
	}
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, false
		}
		if chunk, ok := s.pickLocked(perTableLimit); ok {
			s.mu.Unlock()
			return chunk, true
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (s *chunkScheduler) pickLocked(perTableLimit int) (*Chunk, bool) {
	if len(s.tables) == 0 {
		return nil, false
	}
	n := len(s.tables)
	for i := 0; i < n; i++ {
		idx := (s.rrIndex + i) % n
		tq := s.tables[idx]
		if tq == nil || tq.next >= len(tq.chunks) {
			continue
		}
		if s.activePerTable[tq.key] >= perTableLimit {
			continue
		}
		chunk := tq.chunks[tq.next]
		tq.next++
		s.activePerTable[tq.key]++
		s.rrIndex = (idx + 1) % n
		return chunk, true
	}
	return nil, false
}

func (s *chunkScheduler) markDone(schema, table string) {
	if s == nil {
		return
	}
	key := tableKey(schema, table)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activePerTable[key] > 0 {
		s.activePerTable[key]--
	}
}

func (s *chunkScheduler) tablePending(key string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tq := range s.tables {
		if tq == nil || tq.key != key {
			continue
		}
		pending := len(tq.chunks) - tq.next
		pending += s.activePerTable[key]
		return pending
	}
	return 0
}

func (s *chunkScheduler) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *chunkScheduler) pendingChunks() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := 0
	for _, tq := range s.tables {
		if tq == nil {
			continue
		}
		pending += len(tq.chunks) - tq.next
		for _, n := range s.activePerTable {
			_ = n
		}
		if s.activePerTable[tq.key] > 0 {
			pending += s.activePerTable[tq.key]
		}
	}
	return pending
}
