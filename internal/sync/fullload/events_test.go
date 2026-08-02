package fullload

import (
	"sync"
	"testing"
)

type recordingSink struct {
	mu     sync.Mutex
	events []FullLoadEvent
}

func (r *recordingSink) Emit(ev FullLoadEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingSink) codes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		out[i] = e.Code
	}
	return out
}

func TestEmitNilSinkNoPanic(t *testing.T) {
	Emit(nil, FullLoadEvent{Code: EventCodeTablePlanCreated, Message: "x", Category: EventCategoryTable, Severity: EventSeverityInfo})
}

func TestQueueBackpressureStateTransitions(t *testing.T) {
	sink := &recordingSink{}
	var st queueBackpressureState

	st.observe(sink, 900, 1000)
	codes := sink.codes()
	if len(codes) != 1 || codes[0] != EventCodeQueueBackpressureHigh {
		t.Fatalf("expected high backpressure event, got %v", codes)
	}

	st.observe(sink, 400, 1000)
	codes = sink.codes()
	if len(codes) != 2 || codes[1] != EventCodeQueueBackpressureRecovered {
		t.Fatalf("expected recovered event, got %v", codes)
	}
}

func TestOptionsEffectiveMessage(t *testing.T) {
	opt := ResolveOptions(RawOptions{ReadWorkers: 4, WriteWorkers: 2, BatchSize: 1000})
	msg := optionsEffectiveMessage(opt)
	if msg == "" {
		t.Fatal("expected non-empty message")
	}
}
