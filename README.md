# PesaDB --- A Minimal WAL-Based Relational Database

> **PesaDB** is a compact, educational relational database written in C
> and Python. It implements core RDBMS features---typed tables, CRUD
> operations, primary/unique constraints, and hash joins---backed by a
> crash-safe **Write-Ahead Logging (WAL)** engine. Designed for clarity
> and learning, it skips complex parsing/planning layers in favor of
> direct execution.

------------------------------------------------------------------------

## 🧠 Architecture Overview

Unlike traditional databases with full SQL parsers and query planners,
PesaDB uses a **flat execution model**:

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

-   **No parser**: Input is split on whitespace; no AST or grammar.
-   **No planner**: Commands map directly to method calls.
-   **Yes to storage**: Full WAL-based page management with ACID
    semantics.

This makes PesaDB ideal for **learning**, **debugging**, and
**incremental extension**.

------------------------------------------------------------------------

## 🔒 WAL Engine: The Three Rules

1.  **Never overwrite main data before commit**
2.  **All committed changes live in the log**
3.  **Readers use snapshot boundaries**

Transactions commit when a commit marker is written and `fsync()`
succeeds, enforcing full **ACID** guarantees.

------------------------------------------------------------------------

## 📦 WAL Record Structure

### Page Record

``` c
typedef struct {
    uint32_t type;
    uint32_t tx_id;
    uint32_t page_id;
    uint8_t  data[4096];
} WalPageRecord;
```

### Commit Record

``` c
typedef struct {
    uint32_t type;
    uint32_t tx_id;
    uint32_t magic;
} WalCommitRecord;
```

------------------------------------------------------------------------

## 🔄 Data Flow

-   **Write**: Modify page → append to WAL → commit
-   **Read**: Snapshot WAL → scan backward → fallback to DB
-   **Recovery**: Replay committed pages only
-   **Checkpointing**: Flush committed pages to DB every N writes

------------------------------------------------------------------------

## 🗃️ Data Model

-   Types: `INT`, `TEXT`
-   Constraints: Primary Key, Unique
-   Storage: JSON rows in 4096-byte pages

------------------------------------------------------------------------

## 🛠️ Build & Run

``` bash
make
python repl.py
```

### Example

``` sql
INS users 1 Alice
INS users 2 Bob
SEL users
```

------------------------------------------------------------------------

## 🌐 Web UI Mode

``` bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

⚠️ Insert at least one row after creating tables to ensure catalog
checkpointing.

------------------------------------------------------------------------

## 📁 Project Structure

    pesadb/
    ├── src/c/
    ├── src/python/
    ├── build/
    ├── data/
    ├── repl.py
    ├── api.py
    ├── frontend/
    └── Makefile

------------------------------------------------------------------------

## 💡 Why This Design?

-   Educational
-   Debuggable
-   Extensible
-   WAL-faithful (SQLite-style)

> **"In WAL, the log is the database."**

------------------------------------------------------------------------

## 📚 References

-   SQLite WAL Internals
-   Write-Ahead Logging
-   Python C API
