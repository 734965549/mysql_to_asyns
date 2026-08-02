package fullload

import (
	"context"
	"testing"
)

func TestChunkSchedulerFairOrder(t *testing.T) {
	s := newChunkScheduler()
	mk := func(schema, table string, n int) []*Chunk {
		out := make([]*Chunk, n)
		for i := 0; i < n; i++ {
			out[i] = &Chunk{
				ID: chunkID(&TableSpec{SourceSchema: schema, SourceTable: table}, i),
				Spec: &TableSpec{SourceSchema: schema, SourceTable: table},
			}
		}
		return out
	}
	s.addTable("db", "a", mk("db", "a", 2))
	s.addTable("db", "b", mk("db", "b", 2))
	s.addTable("db", "c", mk("db", "c", 2))

	ctx := context.Background()
	var order []string
	for i := 0; i < 6; i++ {
		chunk, ok := s.next(ctx, 1)
		if !ok || chunk == nil {
			t.Fatalf("next %d failed", i)
		}
		order = append(order, chunk.Spec.SourceTable)
		s.markDone(chunk.Spec.SourceSchema, chunk.Spec.SourceTable)
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d]=%s want %s full=%v", i, order[i], want[i], order)
		}
	}
}

func TestChunkSchedulerSingleTableBurst(t *testing.T) {
	s := newChunkScheduler()
	chunks := []*Chunk{
		{ID: "db.t#0", Spec: &TableSpec{SourceSchema: "db", SourceTable: "t"}},
		{ID: "db.t#1", Spec: &TableSpec{SourceSchema: "db", SourceTable: "t"}},
		{ID: "db.t#2", Spec: &TableSpec{SourceSchema: "db", SourceTable: "t"}},
	}
	s.addTable("db", "t", chunks)
	ctx := context.Background()
	active := 0
	for active < 3 {
		chunk, ok := s.next(ctx, 3)
		if !ok {
			t.Fatalf("expected burst dispatch at active=%d", active)
		}
		active++
		_ = chunk
	}
}
