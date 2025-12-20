┌────────────────────────────────────────┐
       │           BUFFER POOL MANAGER          │
       │ (I want Table 100, Page 5! Get it!)    │
       └───────────────────┬────────────────────┘
                           │ 1. ReadPage(Table: 100, Page: 5)
                           ▼
       ┌────────────────────────────────────────┐
       │             DISK MANAGER               │◄─── ENTRY POINT
       │                                        │
       │  A. Calculate Path: "100.toydb"        │
       │  B. Calculate Offset: 5 * 4096         │
       │  C. Need File Handle... Ask VFD!       │
       │                                        │
       │          ┌──────────────────┐          │
       │          │ 2. Get("100.db") │          │
       │          └────────┬─────────┘          │
       └───────────────────│────────────────────┘
                           │
                           ▼
       ┌────────────────────────────────────────┐
       │               VFD CACHE                │◄─── THE GATEKEEPER
       │           (Limit: 500 Files)           │
       │                                        │
       │  [ Map: "100.db" -> FilePtr ]          │
       │  [ LRU List: 100 -> 50 -> 99 ]         │
       │                                        │
       │  Logic:                                │
       │  - Is "100.db" open? Yes!              │
       │  - Move to Front of List.              │
       │  - Return *os.File pointer.            │
       └───────────────────┬────────────────────┘
                           │ 3. Returns *os.File
                           │
       ┌───────────────────▼────────────────────┐
       │             DISK MANAGER               │
       │                                        │
       │  D. Received valid *os.File            │
       │  E. file.ReadAt(buf, 20480)            │◄─── ACTUAL I/O
       └───────────────────┬────────────────────┘
                           │
                           ▼
       ┌────────────────────────────────────────┐
       │            OPERATING SYSTEM            │
       │            (Hard Drive / SSD)          │
       └────────────────────────────────────────┘