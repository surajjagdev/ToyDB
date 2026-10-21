package transaction

// IsolationLevel dictates how the transaction handles visibility and conflicts.
type IsolationLevel uint8

const (
	// default
	ReadCommitted IsolationLevel = iota + 1

	// repeatable read
	SnapshotIsolation
)

// BeginOptions configures a new transaction before it starts.
type BeginOptions struct {
	// IsolationLevel selects the visibility and conflict strategy.
	// If zero, the TransactionManager will default to SnapshotIsolation.
	IsolationLevel IsolationLevel

	// ReadOnly is a performance optimization. If true, the transaction
	// will not be assigned a physical TransactionID (saving XID space)
	// and will be forbidden from writing to the WAL or tables.
	ReadOnly bool
}
