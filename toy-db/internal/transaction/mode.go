package transaction

type TransactionMode uint8

const (
	// Optimistic: writes proceed without row locks. Conflicts are detected
	// when a writer tries to set xmax on a tuple already being modified by
	// another transaction, or at commit time. The transaction aborts with
	// ErrSerializationFailure; the client retries.
	Optimistic TransactionMode = iota + 1

	// Pessimistic: writes acquire an exclusive lock on the target row,
	// held to commit. Conflicts block rather than abort. This is the
	// "SELECT FOR UPDATE is implicit on every write" mode.
	Pessimistic
)

// BeginOptions configures a new transaction.
type BeginOptions struct {
	// Mode selects the write-side strategy. Zero means Optimistic.
	Mode TransactionMode
}
