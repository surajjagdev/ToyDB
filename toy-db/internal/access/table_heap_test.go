package access

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/disk"
	"github.com/surajjagdev/ToyDB/internal/types"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

type testLogFlusherHeap struct{}

func (testLogFlusherHeap) FlushUpTo(lsn wal.LSN) error { return nil }

func setupTableHeap(t *testing.T, partitions common.PartitionIndex, framesPerPartition common.FrameIndex) (*TableHeap, *buffer.BufferPool) {
	t.Helper()

	dm, err := disk.NewDirectManager(t.TempDir(), 100, 100)
	if err != nil {
		t.Fatalf("Failed to create DirectManager: %v", err)
	}

	bp, err := buffer.NewBufferPool(dm, testLogFlusherHeap{}, partitions, framesPerPartition)
	if err != nil {
		t.Fatalf("Failed to create BufferPool: %v", err)
	}

	rel := common.RelationID(1)
	return NewTableHeap(bp, NewFSM(bp), rel), bp
}

func intSchema(t *testing.T) *catalog.Schema {
	t.Helper()

	schema, err := catalog.NewSchema([]catalog.Column{
		catalog.NewColumn("id", types.IntegerType, false),
	})
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	return schema
}

func insertInt(t *testing.T, th *TableHeap, schema *catalog.Schema, id int32, xmin common.TransactionID) common.RecordId {
	t.Helper()

	rid, err := th.InsertTuple(NewTuple([]types.Value{types.NewInteger(id)}), schema, xmin, 1)
	if err != nil {
		t.Fatalf("InsertTuple(%d) failed: %v", id, err)
	}
	return rid
}

func readInt(t *testing.T, th *TableHeap, schema *catalog.Schema, rid common.RecordId) int32 {
	t.Helper()

	got, err := th.GetTuple(rid, schema)
	if err != nil {
		t.Fatalf("GetTuple(%v) failed: %v", rid, err)
	}
	val, ok := got.Values[0].(types.Fixed4ByteValue)
	if !ok {
		t.Fatalf("expected integer column, got %T", got.Values[0])
	}
	return int32(val.Value)
}

// func writePage(t *testing.T, dm disk.DiskManager, rel common.RelationID, fork common.ForkID, pid common.BlockID) {

// 	var data page.Page = make([]byte, common.PageSize)
// 	for i := 0; i < len(data); i++ {
// 		data[i] = byte(i % 256)
// 	}
// 	data.UpdateChecksum()

// 	err := dm.WritePage(rel, fork, pid, data, false)

// 	if err != nil {
// 		t.Fatalf("Write page failed: %v", err)
// 	}
// }

func TestTableHeap_InsertAndGet(t *testing.T) {
	th, bp := setupTableHeap(t, 2, 8)
	// dm, err := bp.GetDiskManager()
	// if err != nil {
	// 	t.Fatalf("failed to get dm %v", err)
	// }
	// writePage(t,)

	defer bp.Shutdown()

	schema := intSchema(t)
	rid := insertInt(t, th, schema, 42, 1)

	if got := readInt(t, th, schema, rid); got != 42 {
		t.Fatalf("GetTuple = %d, want 42", got)
	}
}

func TestTableHeap_ConcurrentInsertsUniqueAndReadable(t *testing.T) {
	th, bp := setupTableHeap(t, 4, 16)
	defer bp.Shutdown()

	schema := intSchema(t)
	const workers = 1
	const perWorker = 1

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		rids    []common.RecordId
		seen    = make(map[string]struct{})
		failCnt atomic.Int32
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < perWorker; i++ {
				id := int32(worker*1_000_000 + i)
				rid, err := th.InsertTuple(
					NewTuple([]types.Value{types.NewInteger(id)}),
					schema,
					common.TransactionID(worker+1),
					common.CommandID(i+1),
				)
				if err != nil {
					t.Errorf("InsertTuple(%d) failed: %v", id, err)
					failCnt.Add(1)
					return
				}

				key := fmt.Sprintf("%d:%d", rid.BlockID, rid.Slot)
				mu.Lock()
				if _, dup := seen[key]; dup {
					t.Errorf("duplicate record id %s", key)
					failCnt.Add(1)
				}
				seen[key] = struct{}{}
				rids = append(rids, rid)
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("concurrent inserts reported %d failures", failCnt.Load())
	}

	want := workers * perWorker
	if len(rids) != want {
		t.Fatalf("inserted %d tuples, want %d", len(rids), want)
	}

	blocks := make(map[common.BlockID]struct{})
	for _, rid := range rids {
		blocks[rid.BlockID] = struct{}{}
		got, err := th.GetTuple(rid, schema)
		if err != nil {
			t.Fatalf("GetTuple(%v) failed: %v", rid, err)
		}
		if got.Values[0].IsNull() {
			t.Fatalf("unexpected null at %v", rid)
		}
	}

	print("ok")

	// if len(blocks) < 2 {
	// 	t.Fatalf("expected inserts to span multiple heap pages, used %d", len(blocks))
	// }
}

func TestTableHeap_ConcurrentInsertsForceStaleFSMRetry(t *testing.T) {
	// Small pool still has enough frames; contention comes from many writers
	// racing for the same FSM hint on a nearly-full page.
	th, bp := setupTableHeap(t, 2, 16)
	defer bp.Shutdown()

	schema := intSchema(t)
	const workers = 2
	const perWorker = 40

	var wg sync.WaitGroup
	var failCnt atomic.Int32

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := int32(worker*10_000 + i)
				_, err := th.InsertTuple(
					NewTuple([]types.Value{types.NewInteger(id)}),
					schema,
					common.TransactionID(worker+1),
					1,
				)
				if err != nil {
					t.Errorf("InsertTuple(%d) failed: %v", id, err)
					failCnt.Add(1)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("stale-FSM insert race reported %d failures", failCnt.Load())
	}
}

func TestTableHeap_ConcurrentReadersDuringInserts(t *testing.T) {
	th, bp := setupTableHeap(t, 4, 16)
	defer bp.Shutdown()

	schema := intSchema(t)

	var (
		writerWG sync.WaitGroup
		readerWG sync.WaitGroup
		mu       sync.Mutex
		rids     []common.RecordId
		stopRead atomic.Bool
		failCnt  atomic.Int32
	)

	const writers = 6
	const perWriter = 50
	const readers = 6

	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(worker int) {
			defer writerWG.Done()
			for i := 0; i < perWriter; i++ {
				id := int32(worker*1_000 + i)
				rid, err := th.InsertTuple(
					NewTuple([]types.Value{types.NewInteger(id)}),
					schema,
					common.TransactionID(worker+1),
					1,
				)
				if err != nil {
					t.Errorf("InsertTuple(%d) failed: %v", id, err)
					failCnt.Add(1)
					return
				}
				mu.Lock()
				rids = append(rids, rid)
				mu.Unlock()
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for !stopRead.Load() {
				mu.Lock()
				n := len(rids)
				var rid common.RecordId
				if n > 0 {
					rid = rids[n-1]
				}
				mu.Unlock()
				if n == 0 {
					continue
				}
				if _, err := th.GetTuple(rid, schema); err != nil {
					t.Errorf("GetTuple(%v) failed during concurrent inserts: %v", rid, err)
					failCnt.Add(1)
					return
				}
			}
		}()
	}

	writerWG.Wait()
	stopRead.Store(true)
	readerWG.Wait()

	if failCnt.Load() != 0 {
		t.Fatalf("mixed insert/read reported %d failures", failCnt.Load())
	}
}

func TestTableHeap_ConcurrentDeletesOfDistinctTuples(t *testing.T) {
	th, bp := setupTableHeap(t, 4, 16)
	defer bp.Shutdown()

	schema := intSchema(t)
	const n = 200

	rids := make([]common.RecordId, n)
	for i := 0; i < n; i++ {
		rids[i] = insertInt(t, th, schema, int32(i), 1)
	}

	var wg sync.WaitGroup
	var failCnt atomic.Int32

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := th.MarkTupleAsDeleted(rids[i], common.TransactionID(i+2), 1)
			if err != nil {
				t.Errorf("MarkTupleAsDeleted(%v) failed: %v", rids[i], err)
				failCnt.Add(1)
			}
		}(i)
	}

	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("concurrent deletes reported %d failures", failCnt.Load())
	}
}

func TestTableHeap_ConcurrentDeleteSameTupleOneWinner(t *testing.T) {
	th, bp := setupTableHeap(t, 2, 8)
	defer bp.Shutdown()

	schema := intSchema(t)
	rid := insertInt(t, th, schema, 7, 1)

	const workers = 8
	var (
		wg      sync.WaitGroup
		success atomic.Int32
		failCnt atomic.Int32
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			err := th.MarkTupleAsDeleted(rid, common.TransactionID(worker+2), 1)
			if err == nil {
				success.Add(1)
				return
			}
			failCnt.Add(1)
		}(w)
	}

	wg.Wait()

	if success.Load() != 1 {
		t.Fatalf("expected exactly 1 successful delete, got %d (errors=%d)", success.Load(), failCnt.Load())
	}
	if failCnt.Load() != int32(workers-1) {
		t.Fatalf("expected %d failed deletes, got %d", workers-1, failCnt.Load())
	}
}

func TestFSM_ConcurrentRecordAndLookup(t *testing.T) {
	fsmCoord, bp := setupTestFSM(t)
	defer bp.Shutdown()

	rel := common.RelationID(9)
	const workers = 12
	const perWorker = 30

	var wg sync.WaitGroup
	var failCnt atomic.Int32

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				block := common.BlockID(worker*perWorker + i)
				if err := fsmCoord.RecordFreeSpace(rel, block, 1000+i); err != nil {
					t.Errorf("RecordFreeSpace(%d) failed: %v", block, err)
					failCnt.Add(1)
					return
				}
				found, err := fsmCoord.GetBlockWithFreeSpace(rel, 500)
				if err != nil {
					t.Errorf("GetBlockWithFreeSpace failed: %v", err)
					failCnt.Add(1)
					return
				}
				if found == common.InvalidBlockID {
					t.Errorf("expected a block with free space after recording block %d", block)
					failCnt.Add(1)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	if failCnt.Load() != 0 {
		t.Fatalf("concurrent FSM updates reported %d failures", failCnt.Load())
	}
}

func TestTableHeap_Iterator(t *testing.T) {
	th, bp := setupTableHeap(t, 2, 8)
	defer bp.Shutdown()

	schema := intSchema(t)

	for i := range 10 {
		_ = insertInt(t, th, schema, int32(i), common.TransactionID(i))
	}

	it := th.Iterator(schema)
	it.Init()
	tuplesArray := make([]Tuple, 0, 10)

	for {
		tuple, err, isDone := it.Next()

		if isDone {
			break
		}
		if err != nil {
			t.Fatalf("err encountered %v", err)
		}

		tuplesArray = append(tuplesArray, *tuple)
	}

	if len(tuplesArray) != 10 {
		t.Fatalf("expected tuple array os size %d, got %d", len(tuplesArray), 10)
	}

	for i := range 10 {
		val, ok := tuplesArray[i].Values[0].(types.Fixed4ByteValue)
		if !ok {
			t.Fatalf("expected integer column, got %T", tuplesArray[i].Values[0])
		}

		if int32(val.Value) != int32(i) {
			t.Fatalf("expected value of %v, got %v", int32(i), int32(val.Value))
		}
	}
}
