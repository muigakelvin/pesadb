# PesaDB --- A Minimal WAL-Based Relational Database

> **PesaDB** is a compact, educational relational database written in
> **C and Python**. It implements core RDBMS features---typed tables,
> CRUD operations, primary and unique constraints, indexing, and hash
> joins---backed by a crash-safe **Write-Ahead Logging (WAL)** engine.
>
> The project is intentionally **low-level and explicit**: no SQL
> parser, no optimizer, no hidden layers---just direct execution wired
> into a real WAL-backed storage engine.

------------------------------------------------------------------------

## 🧠 Architecture Overview

Unlike traditional databases with full SQL parsers and planners, PesaDB
uses a **flat execution model**:

    ┌──────────┐
    │   REPL   │ ← Interactive shell (`repl.py`)
    └────┬─────┘
         │
    ┌────▼──────────────────────┐
    │ Manual Tokenization       │ ← `line.split()` + hardcoded dispatch
    └────┬──────────────────────┘
         │
    ┌────▼─────┐
    │ Executor │ ← Python execution layer (`executor.py`)
    └────┬─────┘
         │
    ┌────▼──────────────┐
    │ Storage Engine    │ ← C WAL engine (`wal_db_upgraded.c`)
    └───────────────────┘

### Design Characteristics

-   **No SQL parser**: Commands are tokenized via whitespace splitting
-   **No query planner**: Each command maps directly to executor logic
-   **Physical WAL storage**: Full page images written to log
-   **Snapshot isolation**: Readers see consistent database views
-   **Minimal abstractions**: Every system boundary is visible

This makes PesaDB ideal for **learning how databases actually work**,
debugging persistence issues, and incrementally extending features.

------------------------------------------------------------------------

## 🔒 Write-Ahead Logging (WAL): Core Rules

PesaDB enforces durability and isolation through three WAL invariants:

1.  **Never overwrite main data before commit**\
    The main database file (`data.pesa`) is only modified during
    **checkpointing** or **crash recovery**.

2.  **All committed changes live in the log**\
    Every modified page is appended as a **full 4096-byte image** to the
    WAL file (`data.pesa-wal`).

3.  **Readers operate on snapshots**\
    When a reader starts, it records the current WAL size and only
    observes entries **up to that offset**.

> Until checkpointing occurs, **the WAL is the database**.

------------------------------------------------------------------------

## 🔐 Transaction Semantics & ACID

### What Does "Commit" Mean?

A transaction is considered **committed** when:

-   All modified page images are written to the WAL
-   A **commit marker** is appended
-   `fsync(wal_fd)` completes successfully

### ACID Guarantees

  Property      Guarantee
  ------------- ------------------------------------------
  Atomicity     Either all page writes commit or none do
  Consistency   Only valid database states reach disk
  Isolation     Readers never observe partial writes
  Durability    Committed data survives crashes

If a crash occurs **before** the commit marker is written, the
transaction is **discarded entirely** during recovery.

------------------------------------------------------------------------

## 📦 WAL Record Structures (Physical Logging)

PesaDB uses **physical logging** rather than logical SQL logging.

### Page Record

``` c
typedef struct {
    uint32_t type;        // WAL_PAGE = 1
    uint32_t tx_id;       // Transaction ID
    uint32_t page_id;     // Page number in DB
    uint8_t  data[4096];  // Full page image
} WalPageRecord;
```

### Commit Record

``` c
typedef struct {
    uint32_t type;        // WAL_COMMIT = 2
    uint32_t tx_id;
    uint32_t magic;       // 0xC0DECAFE
} WalCommitRecord;
```

**Critical detail:** page records are written **before** the commit
marker.\
Missing commit marker ⇒ transaction is ignored during recovery.

------------------------------------------------------------------------

## 🔄 WAL Data Flow

### Write Path

1.  Load page from main DB into memory
2.  Modify page in RAM (insert/update/delete)
3.  Append page image to WAL
4.  Append commit record
5.  `fsync()` WAL

### Read Path

1.  Reader starts → records WAL size as snapshot
2.  To read a page:
    -   Scan WAL **backward** from snapshot
    -   Use newest committed page record if found
    -   Otherwise read from main DB file

### Crash Recovery

On startup:

1.  Scan WAL forward
2.  Track committed transaction IDs
3.  Replay **only committed pages**
4.  Ignore incomplete transactions

### Checkpointing

Triggered every **N writes** (currently 10):

1.  Determine oldest active reader snapshot
2.  Flush committed pages older than snapshot to main DB
3.  Safely truncate WAL

This ensures bounded WAL growth while preserving snapshot isolation.

------------------------------------------------------------------------

## 🗃️ Data Model

### Supported Column Types

-   `INT` --- 64-bit signed integer
-   `TEXT` --- UTF-8 encoded string

### Constraints

-   **Primary Key** (exactly one per table)
-   **Unique constraints** (optional per column)

### Row Storage

-   Rows serialized as **JSON**
-   Stored inside **4096-byte pages**
-   Deleted rows marked as:

``` json
{"__deleted__": true}
```

------------------------------------------------------------------------

## 🔗 Joins

-   Hash joins implemented in **C** via Python C API
-   Executor builds hash table on join key
-   Join results streamed back to Python layer
-   Supports equality joins only (by design)

Example:

``` sql
JOIN users orders ON id user_id
```

------------------------------------------------------------------------

## 🖥️ Running PesaDB

### Requirements

-   GCC (C11)
-   Python 3.x + headers (`python3-dev`)

### Build

``` bash
make
```

------------------------------------------------------------------------

## ▶️ Mode 1: REPL (No UI)

``` bash
python repl.py
```

Example session:

``` sql
INS users 1 Alice
INS users 2 Bob
SEL users
exit
```

✅ Data persists across restarts because: - WAL recovery replays
committed writes - Tables are created only if missing - Checkpointing
flushes pages to `data.pesa`

------------------------------------------------------------------------

## 🌐 Mode 2: Web UI (FastAPI + React)

Start backend:

``` bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

⚠️ **Important persistence caveat**

-   `api.py` currently **recreates tables on startup if missing**
-   If the catalog exists only in WAL (no checkpoint yet):
    -   Tables appear missing
    -   API recreates them
    -   Old data becomes orphaned

### How to Avoid This

✅ Always insert at least **one row after creating tables** (forces
checkpoint)\
✅ Or change API logic:

``` python
if not db.tables:
    initialize_schema()
```

------------------------------------------------------------------------

## 📁 Project Structure

    pesadb/
    ├── src/c/
    │   ├── wal_db_upgraded.c  # WAL engine
    │   ├── hashjoin.c         # Hash join (C)
    │   └── waldb.h
    ├── src/python/
    │   └── executor.py       # Tables, indexes, bindings
    ├── build/
    │   └── libwaldb.so
    ├── data/
    │   ├── data.pesa
    │   └── data.pesa-wal
    ├── repl.py
    ├── api.py
    ├── frontend/
    └── Makefile

------------------------------------------------------------------------

## 💡 Why This Design?

-   **Educational**: Demonstrates real WAL mechanics
-   **Debuggable**: No hidden planners or optimizers
-   **Faithful**: Mirrors SQLite-style physical WAL
-   **Extensible**: Easy to add B-Trees, SQL parsing, MVCC

> **"In WAL, the log is the database."**\
> The main DB file is merely a cached checkpoint.

------------------------------------------------------------------------

## 📚 References

-   SQLite WAL Internals
-   Write-Ahead Logging (Wikipedia)
-   Python C API Documentation
