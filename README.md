# PesaDB — A Minimal WAL-Based Relational Database

> **PesaDB** is a compact, educational relational database written in C and Python. It implements core RDBMS features—typed tables, CRUD operations, primary/unique constraints, and hash joins—backed by a crash-safe **Write-Ahead Logging (WAL)** engine. Designed for clarity and learning, it skips complex parsing/planning layers in favor of direct execution.

---

## 🧠 Architecture Overview

Unlike traditional databases with full SQL parsers and query planners, PesaDB uses a **flat execution model**:

```
┌──────────┐
│   REPL   │ ← Interactive shell (`repl.py`)
└────┬─────┘
     │
┌────▼──────────────────────┐
│ Manual Tokenization       │ ← `line.split()` + hardcoded dispatch
└────┬──────────────────────┘
     │
┌────▼─────┐
│ Executor │ ← Python logic (`executor.py`)
└────┬─────┘
     │
┌────▼──────────────┐
│ Storage Engine    │ ← C WAL implementation (`wal_db_upgraded.c`)
└───────────────────┘
```

- **No parser**: Input is split on whitespace; no AST or grammar.
- **No planner**: Commands map directly to method calls.
- **Yes to storage**: Full WAL-based page management with ACID semantics.

This makes PesaDB ideal for **learning**, **debugging**, and **incremental extension**.

---

## 🔒 WAL Engine: The Three Rules

PesaDB’s durability and consistency come from its **Write-Ahead Log**, which follows three principles:

1. **Never overwrite main data before commit**  
   The main DB file (`*.pesa`) is only modified during **checkpointing** or **recovery**.

2. **All committed changes live in the log**  
   Every modified page is appended as a full 4096-byte image to the WAL (`*.pesa-wal`).

3. **Readers use snapshot boundaries**  
   When a reader starts, it records the current WAL size. It only sees entries **up to that offset**.

### What Does “Commit” Mean?

- **Before commit**: Changes are tentative, invisible to others, discardable.  
- **After commit**: Changes are **durable** (survive crashes) and **visible**.

In WAL terms, a transaction is **committed** when:
- Its **commit marker** is written to the WAL
- `fsync(wal_fd)` succeeds

This is where **ACID** is enforced:

| Property    | Guarantee                                  |
|-------------|--------------------------------------------|
| Atomicity   | All page writes succeed or none do         |
| Consistency | Only valid DB states are persisted         |
| Isolation   | Readers never see partial/uncommitted data |
| Durability  | Committed data survives system crashes     |

---

## 📦 WAL Record Structure

PesaDB uses **physical logging**: it stores raw page images, not logical SQL.

### Page Record
```c
typedef struct {
    uint32_t type;      // = 1 (WAL_PAGE)
    uint32_t tx_id;     // Transaction ID
    uint32_t page_id;   // Page number in DB
    uint8_t  data[4096]; // Full page image
} WalPageRecord;
```

### Commit Record
```c
typedef struct {
    uint32_t type;      // = 2 (WAL_COMMIT)
    uint32_t tx_id;
    uint32_t magic;     // = 0xC0DECAFE
} WalCommitRecord;
```

> **Key nuance**: Page frames are written **before** the commit marker. If the commit marker is missing after a crash, the entire transaction is **discarded**.

---

## 🔄 Data Flow in WAL

### Write Path
1. **Load page** from main DB into memory
2. **Modify page** in RAM (e.g., insert row)
3. **Append page image** to WAL (with `tx_id`, `page_id`)
4. **Write commit marker** + `fsync()`

### Read Path
1. Reader starts → records current WAL size as **snapshot**
2. To read a page:
   - Scan WAL **backward** from snapshot
   - If a **committed** page record exists → use it
   - Else → read from main DB

### Recovery
On startup:
1. Scan WAL forward
2. Track which `tx_id`s have commit markers
3. Replay **only committed** pages into main DB

### Checkpointing
Every 10 writes:
1. Find **oldest active reader’s snapshot**
2. Copy **committed pages beyond that point** into main DB
3. Truncate WAL safely

This prevents unbounded growth while preserving reader consistency.

---

## 🗃️ Data Model

### Supported Types
- `INT`: 64-bit signed integer
- `TEXT`: UTF-8 string

### Constraints
- **Primary Key**: Exactly one per table; enables fast lookup & enforces uniqueness
- **Unique**: Optional per-column constraint

### Storage Format
- Each row is serialized as **JSON**
- Stored in **4096-byte pages**
- Deleted rows marked with `{"__deleted__": true}`

---

## 🛠️ Build & Run

### Requirements
- GCC (C11 support)
- Python 3.x development headers (`python3-dev`)

### Build
```bash
make
```

### Launch REPL
```bash
python repl.py
```

### Example Session
```sql
INS users 1 Alice
INS users 2 Bob
INS orders 101 1 Laptop
INS orders 102 2 Mouse
JOIN users orders ON id user_id
```

Output:
```json
{"id":1,"name":"Alice","order_id":101,"user_id":1,"item":"Laptop"}
{"id":2,"name":"Bob","order_id":102,"user_id":2,"item":"Mouse"}
```

---

## 📁 Project Structure

```
pesadb/
├── src/c/
│   ├── wal_db_upgraded.c  # WAL engine (pages, txns, recovery)
│   ├── hashjoin.c         # C hash join using Python C API
│   └── waldb.h            # Public C API
├── src/python/
│   └── executor.py        # Tables, indexes, ctypes bindings
├── build/
│   └── libwaldb.so        # Compiled shared library
├── data/                  # Auto-created at runtime
│   ├── data.pesa          # Main DB file
│   └── data.pesa-wal      # Write-Ahead Log
├── repl.py                # Entry point
└── Makefile               # Build rules
```

---

## 💡 Why This Design?

- **Educational**: Demonstrates how WAL enables ACID without complex undo/redo
- **Debuggable**: No hidden layers; every step is explicit
- **Extensible**: Easy to add B-trees, query parsing, or multi-threading
- **Realistic**: Mirrors SQLite’s physical logging approach

> **“In WAL, the log is the database.”** Until checkpointing, all truth lives in the WAL.

---

## 📚 References

- [SQLite WAL Internals](https://www.sqlite.org/wal.html)
- [Write-Ahead Logging (Wikipedia)](https://en.wikipedia.org/wiki/Write-ahead_logging)
- [Python C API Documentation](https://docs.python.org/3/c-api/)

---

PesaDB is **not production-ready**, but it’s a **correct**, **minimal**, and **illuminating** implementation of core database concepts. Perfect for builders who want to understand how real systems work—**from the ground up**.

