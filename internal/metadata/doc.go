// Package metadata owns schema inspection and table identity selection.
//
// It decides whether a source table is matched by primary key, unique key, or
// full-column matching. Sync readers and writers consume that decision but do
// not re-query table identity on their own.
package metadata
