package fullload

import (
	"strings"
)

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quotedIdentifiers(cols []string) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = quoteIdentifier(col)
	}
	return strings.Join(parts, ", ")
}

func orderByColumns(cols []string) string {
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = quoteIdentifier(col) + " ASC"
	}
	return strings.Join(parts, ", ")
}
