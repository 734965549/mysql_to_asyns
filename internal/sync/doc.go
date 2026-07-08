// Package sync owns sync execution primitives.
//
// Application code coordinates incremental binlog replay, domain strategy code
// builds row-matching rules, and infrastructure code reads source rows, writes
// target rows, and manages target read-only state. Task lifecycle state remains
// outside this package.
package sync
