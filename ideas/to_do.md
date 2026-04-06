Now that you are wrapping up the **Buffer Pool Manager** (which consists of your **Frames**, **LRU Replacer**, and the **Manager** itself), you have completed the foundational Memory-to-Disk bridge.

To go from this pure storage engine to a fully functioning **ACID Relational Database**, here is the exact roadmap of components you need to implement next, ordered by dependency:

---

### Phase 1: The Missing Storage Structures (Immediate Next Steps)

Right now, you can create a page, put tuples in it, and cache it in memory. But if a user wants to insert a 200-byte tuple, how do you know *which* page has 200 bytes of free space without reading every single file on disk? 

**1. Free Space Map (FSM) - `internal/storage/fsm/`**
*   **What it is:** A specialized tree structure stored in the `common.ForkFSM` file.
*   **Why you need it:** Instead of scanning pages to find space, the FSM tracks the available space on every `ForkMain` block. 
*   **Implementation:** Postgres uses a Max-Heap tree stored inside FSM pages. You query it: *"Give me a BlockID that has at least 250 bytes free."*

**2. Write-Ahead Log (WAL) Manager - `internal/storage/wal/`**
*   **What it is:** The append-only log that guarantees Durability (the 'D' in ACID).
*   **Why you need it:** If the database crashes, your Buffer Pool loses all unwritten dirty frames. 
*   **Implementation:** You need to define log records (e.g., `InsertRecord`, `UpdateRecord`). You will modify your Buffer Pool so that **before a dirty Frame is evicted**, it forces the WAL to flush to disk up to the `Page.GetLSN()`. 

---

### Phase 2: Data Access & Catalog

Once you have FSM and WAL, you can safely write and find data. Now you need to map raw `[]byte` arrays to actual SQL tables.

**3. Type System & Serialization - `internal/types/`**
*   **What it is:** Go representations of `INTEGER`, `VARCHAR`, `BOOLEAN`.
*   **Why you need it:** To convert a Go `string` into the raw `[]byte` that your `HeapPage.InsertTuple` expects.

**4. The Catalog / Schema Manager - `internal/catalog/`**
*   **What it is:** System tables (like Postgres's `pg_class` and `pg_attribute`).
*   **Why you need it:** When the user types `SELECT name FROM users`, the database needs to look up the `RelationID` for `users`, the `ForkID`, and the byte-offset for the `name` column inside the tuple.

**5. Heap Access Method (HeapAM) - `internal/access/heap.go`**
*   **What it is:** The coordinator API. 
*   **Why you need it:** It acts as the glue. It takes a row, asks the Catalog for types, asks the FSM for a block, asks the Buffer Pool for the frame, writes the WAL record, and finally calls your `HeapPage.InsertTuple`.

---

### Phase 3: MVCC & Concurrency (The Brain)

You already added `xmin`, `xmax`, and `cid` to your `HeapPage`. Now you need the engine that generates and checks them.

**6. Transaction Manager - `internal/concurrency/txn_manager.go`**
*   **What it is:** Hands out strictly increasing `TransactionID`s (txid). 
*   **Why you need it:** It tracks which transactions are `ACTIVE`, `COMMITTED`, or `ABORTED`. 

**7. MVCC Snapshot Isolation - `internal/concurrency/visibility.go`**
*   **What it is:** The rules of time-travel. 
*   **Why you need it:** When a query reads a tuple from your `HeapPage`, it looks at `xmin` and `xmax`. It must ask the Snapshot Manager: *"Was `xmin` committed before I started? Is `xmax` still active?"* If yes, the tuple is visible. If no, skip it.

**8. Lock Manager (Optional but recommended) - `internal/concurrency/lock_manager.go`**
*   **What it is:** Handles row-level and table-level locks (`SELECT ... FOR UPDATE`).
*   **Why you need it:** Even with MVCC, if two active transactions try to update the *exact same tuple* at the exact same time, one must wait or abort.

---

### Phase 4: Execution & SQL 

Finally, you expose the database to the outside world.

**9. B-Tree Indexes - `internal/access/index/`**
*   **What it is:** A secondary `RelationID` that maps a key (like a User ID) to a `BlockID` and `SlotID`.
*   **Why you need it:** To avoid full table scans. You will build this using your existing generic `Page` structure (using `PageFlagIndex` instead of `PageFlagHeap`).

**10. Query Executor & Planner - `internal/execution/`**
*   **What it is:** The Volcano Iterator model (`Init()`, `Next()`, `Close()`).
*   **Why you need it:** To run nodes like `SeqScanNode`, `IndexScanNode`, `HashJoinNode`.

**11. SQL Parser - `internal/sql/`**
*   **What it is:** Converts `INSERT INTO...` into a logical query plan.

---

### What to code *right now*?

Assuming your **Buffer Pool Frame** code is almost done, your immediate next task should be the **Write-Ahead Log (WAL) structure and the LSN (Log Sequence Number) generator**. 

You *cannot* test your Buffer Pool eviction properly until you enforce the rule: `if Frame.IsDirty && Frame.Page.GetLSN() > WAL.FlushedLSN { flushWAL() }`. 

Would you like to design the **WAL Log Record** structures next?