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

---

## 🔒 Write-Ahead Logging (WAL): Core Rules

1. **Never overwrite main data before commit**
2. **All committed changes live in the log**
3. **Readers operate on snapshots**

> Until checkpointing occurs, **the WAL is the database**.

---

## 🔐 Transaction Semantics & ACID

A transaction commits only after WAL flush and commit record fsync.

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

## 🌐 Mode 2: Web UI (API + Frontend)

### Step 1: Start Backend API

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

### Step 2: Start Frontend

```bash
cd frontend
npm install
npm run dev
```

Open in browser:

```
http://localhost:5173
```

The frontend communicates with the FastAPI backend to execute database
commands via HTTP.

---

## 📁 Project Structure

```
pesadb/
├── src/c/
│   ├── wal_db_upgraded.c
│   ├── hashjoin.c
│   └── waldb.h
├── src/python/
│   └── executor.py
├── build/
│   └── libwaldb.so
├── data/
│   ├── data.pesa
│   └── data.pesa-wal
├── repl.py
├── api.py
├── frontend/
└── Makefile
```

---

## 💡 Design Rationale

* Educational
* Debuggable
* WAL-first persistence
* Extensible architecture

---

## 📚 References

* SQLite WAL Internals
* Write-Ahead Logging — Wikipedia
* Python C API Documentation
