# PesaDB — A Minimal WAL-Based Relational Database

> **PesaDB** is a compact, educational relational database written in
> **C and Python**. It implements core RDBMS features—typed tables, CRUD
> operations, primary and unique constraints, indexing, and hash joins—
> backed by a crash-safe **Write-Ahead Logging (WAL)** engine.
>
> The system is intentionally **low-level and explicit**:
> no SQL parser, no optimizer, no hidden abstractions—just direct execution
> wired into a real WAL-backed storage engine.

---

## 🧠 Architecture Overview

PesaDB uses a **flat execution model**, avoiding traditional SQL parsing
and planning layers entirely:

```
┌──────────┐
│   REPL   │ ← Interactive shell (repl.py)
└────┬─────┘
     │
┌────▼──────────────────────┐
│ Manual Tokenization       │ ← line.split() + hardcoded dispatch
└────┬──────────────────────┘
     │
┌────▼─────┐
│ Executor │ ← Python execution layer (executor.py)
└────┬─────┘
     │
┌────▼──────────────┐
│ Storage Engine    │ ← C WAL engine (wal_db_upgraded.c)
└───────────────────┘
```

### Key Characteristics

* **No SQL parser** — commands are whitespace-tokenized
* **No query planner** — commands map directly to execution logic
* **Physical WAL** — full-page images logged
* **Snapshot isolation** — readers see consistent views
* **Minimal abstractions** — every boundary is explicit and inspectable

This makes PesaDB ideal for **learning database internals**, debugging
persistence behavior, and controlled extension.

---

## 🔒 Write-Ahead Logging (WAL): Core Rules

PesaDB enforces durability and isolation using three strict WAL
invariants:

1. **Never overwrite main data before commit**
   The main database file (`data.pesa`) is only modified during
   **checkpointing** or **crash recovery**.

2. **All committed changes live in the log**
   Every modified page is appended as a **4096-byte page image** to the WAL
   (`data.pesa-wal`).

3. **Readers operate on snapshots**
   Readers record the WAL size at query start and only observe entries up
   to that offset.

> Until checkpointing occurs, **the WAL is the database**.

---

## 🔐 Transaction Semantics & ACID

### Commit Definition

A transaction is considered **committed** when:

* All modified page images are written to the WAL
* A **commit record** is appended
* `fsync(wal_fd)` completes successfully

If a crash occurs **before** the commit record is written, the transaction
is **discarded** during recovery.

### ACID Guarantees

| Property    | Guarantee                       |
|------------|---------------------------------|
| Atomicity  | All-or-nothing page commits     |
| Consistency| Only valid states persist       |
| Isolation  | No partial reads                |
| Durability | Committed data survives crashes |

---

## 📦 WAL Record Structures (Physical Logging)

### Page Record

```c
typedef struct {
    uint32_t type;        // WAL_PAGE = 1
    uint32_t tx_id;       // Transaction ID
    uint32_t page_id;     // Page number
    uint8_t  data[4096];  // Full page image
} WalPageRecord;
```

### Commit Record

```c
typedef struct {
    uint32_t type;        // WAL_COMMIT = 2
    uint32_t tx_id;
    uint32_t magic;       // 0xC0DECAFE
} WalCommitRecord;
```

Page records are always written **before** the commit record.
A missing commit record ⇒ transaction ignored during recovery.

---

## 🔄 WAL Data Flow

### Write Path
1. Load page into memory
2. Modify page
3. Append page image to WAL
4. Append commit record
5. `fsync()` WAL

### Read Path
1. Record WAL snapshot offset
2. Scan WAL backward
3. Use newest committed page
4. Fallback to main DB

### Crash Recovery
1. Scan WAL forward
2. Identify committed transactions
3. Replay committed pages only

### Checkpointing
* Triggered every **N writes** (currently 10)
* Flushes committed pages to `data.pesa`
* Truncates WAL safely

---

## 🗃️ Data Model

### Types
* `INT` — 64-bit signed integer
* `TEXT` — UTF-8 string

### Constraints
* Exactly one **Primary Key** per table
* Optional **Unique** constraints

### Row Storage
* Rows serialized as **JSON**
* Stored in **4096-byte pages**
* Deleted rows marked as:

```json
{"__deleted__": true}
```

---

## 🔗 Joins

* Equality **hash joins**
* Implemented in **C** via the Python C API
* Hash table built on join key
* Results streamed to the Python executor

Example:
```sql
JOIN users orders ON id user_id
```

---

## 🖥️ Running PesaDB

### Requirements
* GCC (C11)
* Python 3.x + `python3-dev`
* Node.js (for frontend)

### Build
```bash
make
```

---

## ▶️ Mode 1: REPL (No UI)

```bash
python repl.py
```

### Full CRUD Example in REPL
```sql
INS users 1 Alice
INS users 2 Bob
INS orders 101 1 Laptop
INS orders 102 2 Mouse
SEL users
SEL users WHERE id 1
UPD users SET name=Alicia WHERE id=1
DEL orders 102
JOIN users orders ON id user_id
exit
```

---

## 📚 References
* SQLite WAL Internals
* Write-Ahead Logging — Wikipedia
* Python C API Documentation
