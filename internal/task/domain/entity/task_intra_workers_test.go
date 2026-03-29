package entity

import "testing"

func TestEffectiveIntraTableWorkers_LegacyCap(t *testing.T) {
	if g := EffectiveIntraTableWorkers(0, 4, 16, 64); g != 4 {
		t.Fatalf("want 4, got %d", g)
	}
	if g := EffectiveIntraTableWorkers(0, 32, 16, 64); g != 16 {
		t.Fatalf("want 16, got %d", g)
	}
}

func TestEffectiveIntraTableWorkers_Explicit(t *testing.T) {
	if g := EffectiveIntraTableWorkers(24, 4, 16, 64); g != 24 {
		t.Fatalf("want 24, got %d", g)
	}
}

func TestEffectiveIntraTableWorkers_HardMax(t *testing.T) {
	if g := EffectiveIntraTableWorkers(200, 4, 16, 64); g != 64 {
		t.Fatalf("want 64, got %d", g)
	}
}

func TestEffectiveIntraTableWorkers_CustomLegacyCap(t *testing.T) {
	if g := EffectiveIntraTableWorkers(0, 64, 32, 128); g != 32 {
		t.Fatalf("want 32, got %d", g)
	}
}
