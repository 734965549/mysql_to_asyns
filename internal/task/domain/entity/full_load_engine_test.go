package entity

import "testing"

func TestUsesFullLoadV2(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"", false},
		{"v1", false},
		{"V1", false},
		{"v2", true},
		{"V2", true},
		{" v2 ", true},
		{"foo", false},
	}
	for _, c := range cases {
		cfg := &TaskConfig{FullLoadEngine: c.val}
		if got := cfg.UsesFullLoadV2(); got != c.want {
			t.Errorf("UsesFullLoadV2(%q)=%v want %v", c.val, got, c.want)
		}
	}
}
