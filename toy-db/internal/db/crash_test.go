package db_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/surajjagdev/ToyDB/internal/access"
	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/db"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/types"
)

// -----------------------------------------------------------------------------
// Environment variables used to signal "you are the child process"
// -----------------------------------------------------------------------------

const (
	envCrashChild = "TOYDB_CRASH_CHILD"
	envCrashDir   = "TOYDB_CRASH_DIR"
	envCrashPlan  = "TOYDB_CRASH_PLAN" // pipe path from parent to child
	envCrashAcks  = "TOYDB_CRASH_ACKS" // pipe path from child to parent
)

// -----------------------------------------------------------------------------
// Test entry point — dispatches to child or parent
// -----------------------------------------------------------------------------

func TestCrashRecovery(t *testing.T) {
	if os.Getenv(envCrashChild) != "" {
		runCrashChild(t)
		return
	}
	runCrashParent(t)
}

// -----------------------------------------------------------------------------
// Child process
// -----------------------------------------------------------------------------
//
// Reads commands from a fifo at $TOYDB_CRASH_PLAN and acknowledges each
// command by writing to $TOYDB_CRASH_ACKS. The commands are:
//
//   "write N"  — start a transaction, insert rows with ids [N*1000, N*1000+K),
//                commit it, then write "acked write N"
//   "start N"  — start a transaction with id N but DON'T commit it, write
//                "acked start N" — the transaction stays open when the process
//                gets SIGKILLed
//
// The parent uses these commands to construct a deterministic workload that
// ends with the child being killed mid-transaction.

func runCrashChild(t *testing.T) {
	dir := os.Getenv(envCrashDir)
	planPath := os.Getenv(envCrashPlan)
	acksPath := os.Getenv(envCrashAcks)

	if dir == "" || planPath == "" || acksPath == "" {
		fmt.Fprintln(os.Stderr, "child: missing env vars")
		os.Exit(2)
	}

	// Open the DB with fsync ON — this is the whole point of the test.
	d, err := db.Open(context.Background(), db.Options{
		Dir:                dir,
		Fsync:              true,
		CheckpointInterval: time.Hour, // don't checkpoint mid-test
		WALSizeThreshold:   1 << 30,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: db.Open: %v\n", err)
		os.Exit(2)
	}

	schema := intSchemaForChild(t)

	// Open the plan fifo for reading and acks fifo for writing.
	plan, err := os.Open(planPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open plan: %v\n", err)
		os.Exit(2)
	}
	defer plan.Close()

	acks, err := os.OpenFile(acksPath, os.O_WRONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open acks: %v\n", err)
		os.Exit(2)
	}
	defer acks.Close()

	planR := bufio.NewReader(plan)
	acksW := bufio.NewWriter(acks)

	// Track open transactions so we can dump their ids before we're killed.
	openTxIDs := []common.TransactionID{}
	var openTxMu sync.Mutex

	for {
		line, err := planR.ReadString('\n')
		if err != nil {
			// Parent closed the pipe: exit cleanly.
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			fmt.Fprintf(os.Stderr, "child: bad command %q\n", line)
			os.Exit(2)
		}
		cmd := fields[0]
		id, _ := strconv.ParseInt(fields[1], 10, 64)

		switch cmd {
		case "write":
			// Start a transaction, write K rows, commit, ack.
			ctx := context.Background()
			tx, err := d.TM().Begin(ctx, transaction.BeginOptions{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "child: Begin: %v\n", err)
				os.Exit(2)
			}
			heap := d.Heap(1)
			for i := 0; i < 10; i++ {
				tx.NextCommandID()
				rowID := int32(id*1000 + int64(i))
				if _, err := heap.InsertTuple(tx,
					access.NewTuple([]types.Value{types.NewInteger(rowID)}), schema); err != nil {
					fmt.Fprintf(os.Stderr, "child: InsertTuple: %v\n", err)
					os.Exit(2)
				}
			}
			if err := d.TM().Commit(tx); err != nil {
				fmt.Fprintf(os.Stderr, "child: Commit: %v\n", err)
				os.Exit(2)
			}
			// Ack: the commit is durable.
			fmt.Fprintf(acksW, "acked write %d\n", id)
			acksW.Flush()

		case "start":
			// Start a transaction, write one row, DON'T commit. The
			// transaction stays in-flight when we get SIGKILLed.
			ctx := context.Background()
			tx, err := d.TM().Begin(ctx, transaction.BeginOptions{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "child: Begin: %v\n", err)
				os.Exit(2)
			}
			openTxMu.Lock()
			openTxIDs = append(openTxIDs, tx.Id)
			txID := tx.Id
			openTxMu.Unlock()

			tx.NextCommandID()
			rowID := int32(id*1000 + 999)
			heap := d.Heap(1)
			if _, err := heap.InsertTuple(tx,
				access.NewTuple([]types.Value{types.NewInteger(rowID)}), schema); err != nil {
				fmt.Fprintf(os.Stderr, "child: InsertTuple: %v\n", err)
				os.Exit(2)
			}
			// Ack: the transaction is started and its insert is logged.
			fmt.Fprintf(acksW, "acked start %d txid %d\n", id, txID)
			acksW.Flush()
			// NOTE: we intentionally do NOT commit or abort.
		}
	}
}

// intSchemaForChild is a copy of intSchema because the parent's version isn't
// available to the child (different process, no shared state).
func intSchemaForChild(t *testing.T) *catalog.Schema {
	s, err := catalog.NewSchema([]catalog.Column{
		catalog.NewColumn("id", types.IntegerType, false),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: catalog.NewSchema: %v\n", err)
		os.Exit(2)
	}
	return s
}

// -----------------------------------------------------------------------------
// Parent process
// -----------------------------------------------------------------------------

// runCrashParent orchestrates the child, kills it, and verifies recovery.
func runCrashParent(t *testing.T) {

	dir := t.TempDir()

	// Set up two named pipes (fifos) for plan and acks.
	planPath := dir + "/plan.pipe"
	acksPath := dir + "/acks.pipe"
	if err := syscall.Mkfifo(planPath, 0600); err != nil {
		t.Fatalf("mkfifo plan: %v", err)
	}
	if err := syscall.Mkfifo(acksPath, 0600); err != nil {
		t.Fatalf("mkfifo acks: %v", err)
	}

	// Spawn the child process.
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$", "-test.v")
	cmd.Env = append(os.Environ(),
		envCrashChild+"=1",
		envCrashDir+"="+dir,
		envCrashPlan+"="+planPath,
		envCrashAcks+"="+acksPath,
	)
	cmd.Stdout = os.Stderr // child test logs go to stderr so we can see failures
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() {
		// No-op if child already exited.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// The child process will block on opening planPath for reading until we
	// open it for writing. Same for acks in the other direction.
	plan, err := os.OpenFile(planPath, os.O_WRONLY, 0)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("open plan: %v", err)
	}
	defer plan.Close()

	acks, err := os.Open(acksPath)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("open acks: %v", err)
	}
	defer acks.Close()

	planW := bufio.NewWriter(plan)
	acksR := bufio.NewReader(acks)

	// sendAndWait writes a command to the child and waits for the matching
	// ack line.
	sendAndWait := func(cmdLine string) string {
		if _, err := fmt.Fprintln(planW, cmdLine); err != nil {
			t.Fatalf("write plan: %v", err)
		}
		if err := planW.Flush(); err != nil {
			t.Fatalf("flush plan: %v", err)
		}
		ack, err := acksR.ReadString('\n')
		if err != nil {
			t.Fatalf("read ack for %q: %v", cmdLine, err)
		}
		return strings.TrimSpace(ack)
	}

	// 1. Ten committed transactions, each acked.
	const committedTx = 10
	for i := 1; i <= committedTx; i++ {
		ack := sendAndWait(fmt.Sprintf("write %d", i))
		want := fmt.Sprintf("acked write %d", i)
		if ack != want {
			t.Fatalf("ack for write %d: got %q, want %q", i, ack, want)
		}
	}

	// 2. Three started-but-not-committed transactions. These will be
	//    undone by recovery.
	const uncommittedTx = 3
	var uncommittedTxIDs []common.TransactionID
	for i := 1; i <= uncommittedTx; i++ {
		ack := sendAndWait(fmt.Sprintf("start %d", i))
		// ack is "acked start N txid M"
		fields := strings.Fields(ack)
		if len(fields) != 5 || fields[0] != "acked" || fields[1] != "start" {
			t.Fatalf("bad start ack: %q", ack)
		}
		txid, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			t.Fatalf("parse txid from %q: %v", ack, err)
		}
		uncommittedTxIDs = append(uncommittedTxIDs, common.TransactionID(txid))
	}

	// 3. SIGKILL the child. No cleanup runs. The WAL is durable (fsync
	//    was on), the data pages are in the abandoned buffer pool.
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()

	// 4. Reopen the DB and run recovery.
	d, err := db.Open(context.Background(), db.Options{
		Dir:                dir,
		Fsync:              true,
		CheckpointInterval: time.Hour,
		WALSizeThreshold:   1 << 30,
		LogProgress:        testing.Verbose(),
	})
	if err != nil {
		t.Fatalf("parent reopen: %v", err)
	}
	defer d.Close(context.Background())

	rec := d.RecoveryResult()
	if rec == nil {
		t.Fatal("no RecoveryResult")
	}

	// 5. Verify recovery classified transactions correctly.
	t.Logf("recovery: %d txns analyzed, %d committed, %d aborted, %d losers",
		len(rec.Analysis.TxTable),
		rec.Analysis.CommittedCount(),
		rec.Analysis.AbortedCount(),
		len(rec.Analysis.Losers),
	)

	if rec.Analysis.CommittedCount() < committedTx {
		t.Errorf("committed = %d, want >= %d", rec.Analysis.CommittedCount(), committedTx)
	}

	// The uncommitted transactions should have been rolled back. Their
	// txids should not be in the committed set.
	committedIDs := make(map[common.TransactionID]bool)
	for txid, entry := range rec.Analysis.TxTable {
		if entry.Status == 0 { // TxActive (shouldn't happen after undo)
			committedIDs[txid] = false
			continue
		}
		committedIDs[txid] = entry.Status == 1 // TxCommitted
	}
	for _, txid := range uncommittedTxIDs {
		if committedIDs[txid] {
			t.Errorf("uncommitted tx %d is marked committed after recovery", txid)
		}
	}

	// 6. Verify the actual data: every row from a committed transaction
	//    must be readable; every row from an uncommitted one must be gone.
	schema := intSchema(t)
	ctx := context.Background()

	tx, err := d.TM().Begin(ctx, transaction.BeginOptions{})
	if err != nil {
		t.Fatalf("verify tx: %v", err)
	}
	defer d.TM().Commit(tx)

	heap := d.Heap(1)

	// Committed rows: 10 transactions × 10 rows each = 100 rows.
	// Visible via iterator.
	it := heap.Iterator(tx, schema)
	it.Init()
	seen := make(map[int32]bool)
	for {
		tuple, err, done := it.Next()
		if done {
			break
		}
		if err != nil {
			t.Fatalf("iterator: %v", err)
		}
		v, ok := tuple.Values[0].(types.Fixed4ByteValue)
		if !ok {
			t.Fatalf("expected int, got %T", tuple.Values[0])
		}
		seen[int32(v.Value)] = true
	}

	// Every committed row must be visible.
	for w := int32(1); w <= int32(committedTx); w++ {
		for i := int32(0); i < 10; i++ {
			rowID := w*1000 + i
			if !seen[rowID] {
				t.Errorf("committed row %d missing after recovery", rowID)
			}
		}
	}

	// Every uncommitted row must NOT be visible.
	for u := int32(1); u <= int32(uncommittedTx); u++ {
		rowID := u*1000 + 999
		if seen[rowID] {
			t.Errorf("uncommitted row %d visible after recovery (should be rolled back)", rowID)
		}
	}

	// Count: exactly 100 rows visible.
	wantTotal := committedTx * 10
	if len(seen) != wantTotal {
		t.Errorf("visible rows = %d, want %d", len(seen), wantTotal)
	}
}
