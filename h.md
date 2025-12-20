# Go MVCC Database Implementation Plan

## Architecture Overview

The database will use a layered architecture:

```
┌─────────────────────────────────────────┐
│         SQL Parser & Executor           │
├─────────────────────────────────────────┤
│    Transaction Manager (MVCC + OCC)     │
├─────────────────────────────────────────┤
│   Catalog & Metadata Management         │
├─────────────────────────────────────────┤
│   Storage Layer (Page Manager + WAL)    │
└─────────────────────────────────────────┘
```

## Project Structure

db/
├── cmd/db/main.go
├── pkg/                 # Public APIs (Drivers)
│   └── driver.go
├── internal/
│   ├── common/          # Types used by EVERYONE (Imported by all below)
│   │   ├── config.go
│   │   ├── types.go
│   │   └── util.go
│   ├── storage/
│   │   ├── disk/        # os.File wrappers
│   │   ├── page/        # Raw byte manipulation + Checksums
│   │   ├── wal/         # Log Record definitions + rotation
│   │   └── buffer/      # LRU + Pin/Unpin
│   ├── access/          # High-level Data Access (Import storage)
│   │   ├── tuple.go     # Xmin/Xmax logic
│   │   ├── heap.go      # Insert/Update/Delete (MVCC logic)
│   │   └── index/       # B+Tree
│   ├── concurrency/     # (Imports access)
│   │   ├── transaction.go
│   │   ├── manager.go
│   │   └── conflict.go  # OCC Validation
│   ├── catalog/         # (Imports access + concurrency)
│   │   └── schema.go
│   └── execution/       # (Imports everything above)
│       ├── executor.go
│       └── sql/

## Key Components

### 1. Storage Layer (`internal/storage/`)

**Page Manager:**

- Fixed-size pages (e.g., 4KB or 8KB)
- Page ID allocation and deallocation
- Page-level read/write operations
- File-based storage with mmap or direct I/O

**Buffer Pool:**

- LRU cache for frequently accessed pages
- Dirty page tracking
- Page eviction policies

**Write-Ahead Log (WAL):**

- Append-only log file
- Log entries: BEGIN, COMMIT, ABORT, UPDATE, INSERT, DELETE
- Checkpoint mechanism for log truncation
- Recovery from WAL on startup

### 2. MVCC Implementation (`internal/transaction/`)

**Version Management:**

- Each row stores multiple versions with timestamps (transaction IDs)
- Read operations access versions visible to current transaction's snapshot
- Write operations create new versions (copy-on-write)
- Garbage collection for old versions

**Transaction Manager:**

- Transaction ID allocation (monotonically increasing counter)
- Snapshot isolation: transactions see consistent snapshot at start
- Transaction state tracking (active, committed, aborted)

**Optimistic Concurrency Control:**

- Validation phase before commit
- Check for read/write conflicts with other concurrent transactions
- Abort and retry on conflict detection

### 3. Catalog System (`internal/catalog/`)

**Schema Storage:**

- Table definitions (name, columns, types)
- Column metadata (name, type, constraints)
- Index definitions (which columns, B-tree references)
- Stored in system tables (e.g., `pg_catalog`-style)

### 4. SQL Layer (`internal/sql/`)

**Parser:**

- Use a SQL parser library (e.g., `github.com/xwb1989/sqlparser`) or hand-write basic parser
- Parse: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX

**Executor:**

- Query planning (basic query optimizer)
- Execution engine with operators (Scan, Filter, Project, Join, Aggregate)
- Type coercion and validation

### 5. Table Storage (`internal/storage/table.go`)

**Row Format:**

```
[Version Info][NULL bitmap][Column1][Column2]...[ColumnN]
- Version Info: TX ID, prev/next version pointers
- NULL bitmap: 1 bit per nullable column
- Columns: Variable-length encoding
```

**Version Chain:**

- Linked list of row versions per logical row
- Newer versions point to older versions
- Garbage collection for committed versions older than oldest active transaction

### 6. Index Support (`internal/storage/btree.go`)

- B-tree index structure
- Index entries point to row versions
- Support for unique constraints
- Range queries and point lookups

## Implementation Steps

### Phase 1: Foundation

1. Set up Go module and project structure
2. Implement basic page-based storage (page.go, file.go)
3. Implement WAL with basic operations (BEGIN, COMMIT, UPDATE)
4. Implement buffer pool with LRU eviction

### Phase 2: Catalog & Types

5. Define type system (INTEGER, BIGINT, TEXT, VARCHAR, BOOLEAN, FLOAT)
6. Implement catalog for table/column metadata
7. Implement system tables for catalog storage

### Phase 3: Transaction Management

8. Implement transaction manager with TX ID allocation
9. Implement MVCC version management (row versions with TX IDs)
10. Implement snapshot isolation (visible version selection)
11. Implement OCC validation and conflict detection

### Phase 4: Table Operations

12. Implement row format with version info
13. Implement INSERT, UPDATE, DELETE with version creation
14. Implement SELECT with version visibility filtering
15. Implement garbage collection for old versions

### Phase 5: SQL Interface

16. Implement SQL parser (basic DML + DDL)
17. Implement query executor with basic operators
18. Implement CREATE TABLE, CREATE INDEX DDL

### Phase 6: Indexing & Optimization

19. Implement B-tree index structure
20. Integrate indexes with query executor
21. Add index-based lookups and range scans

### Phase 7: Recovery & Persistence

22. Implement WAL recovery on database startup
23. Implement checkpoint mechanism
24. Add crash recovery tests

## Technical Decisions

- **SQL Parser**: Use `github.com/xwb1989/sqlparser` for SQL parsing (supports MySQL syntax, can be adapted)
- **Transaction IDs**: 64-bit integers, stored in separate file or header page
- **Version Storage**: Versions stored inline in pages with forward/backward pointers
- **Conflict Detection**: Use read set and write set tracking in OCC validation
- **Lock-Free Reads**: MVCC allows lock-free read operations (snapshot isolation)

## File Organization

Key files to implement:

1. `internal/storage/page.go` - Page structure, allocation, serialization
2. `internal/storage/wal.go` - WAL entry types, append, recovery
3. `internal/storage/buffer.go` - Buffer pool with LRU
4. `internal/transaction/manager.go` - Main transaction coordinator
5. `internal/transaction/version.go` - MVCC version chain management
6. `internal/storage/table.go` - Table heap, row storage, version chains
7. `internal/storage/row.go` - Row encoding/decoding with version metadata
8. `internal/catalog/schema.go` - Schema definitions and validation
9. `internal/sql/executor.go` - Query execution operators
10. `internal/types/value.go` - Value types and serialization

## Testing Strategy

- Unit tests for each component
- Integration tests for transaction scenarios
- Concurrency tests for OCC conflict detection
- Crash recovery tests with WAL replay