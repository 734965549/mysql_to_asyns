package binlog

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildIncludeTableRegex(t *testing.T) {
	tests := []struct {
		name      string
		databases []string
		tables    []string
		matches   []string
		rejects   []string
		count     int
	}{
		{
			name: "all tables in every database", databases: []string{"db1", "db2"}, count: 2,
			matches: []string{"db1.users", "db2.audit"}, rejects: []string{"db3.users"},
		},
		{
			name: "selected tables in every database", databases: []string{"db1", "db2"}, tables: []string{"users", "orders"}, count: 4,
			matches: []string{"db1.users", "db1.orders", "db2.users", "db2.orders"}, rejects: []string{"db2.audit"},
		},
		{
			name: "regex metacharacters are literal", databases: []string{"db.prod"}, tables: []string{"order+items"}, count: 1,
			matches: []string{"db.prod.order+items"}, rejects: []string{"dbXprod.orderitems"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patterns := buildIncludeTableRegex(tt.databases, tt.tables)
			require.Len(t, patterns, tt.count)
			for _, value := range tt.matches {
				matched := false
				for _, pattern := range patterns {
					if regexp.MustCompile(pattern).MatchString(value) {
						matched = true
						break
					}
				}
				assert.True(t, matched, "%s should match one of %v", value, patterns)
			}
			for _, value := range tt.rejects {
				for _, pattern := range patterns {
					assert.False(t, regexp.MustCompile(pattern).MatchString(value), "%s unexpectedly matched %s", value, pattern)
				}
			}
		})
	}
}
