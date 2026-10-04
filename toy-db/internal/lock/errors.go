package lock

import "errors"

var (
	ErrLockTimeout = errors.New("lock: wait timeout")
	// ErrDeadlock is returned when a cycle is detected in the wait-for graph.
	ErrDeadlock = errors.New("lock: deadlock detected")
)
