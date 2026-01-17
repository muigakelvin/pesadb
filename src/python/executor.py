import ctypes
import json
from typing import List, Dict, Any, Optional
from enum import Enum
import os

# ----------------------------
# Load C library from build/ directory (relative to this file)
# ----------------------------
_lib_path = os.path.join(os.path.dirname(__file__), "../../build/libwaldb.so")
if not os.path.isfile(_lib_path):
    raise RuntimeError(
        f"Shared library not found at {_lib_path}. "
        "Please run 'make' from the project root to build it."
    )
_lib = ctypes.CDLL(_lib_path)

# Define types
c_txn = ctypes.c_void_p
c_char_pp = ctypes.POINTER(ctypes.c_char_p)

# ----------------------------
# Bind WAL/DB functions
# ----------------------------
open_db = _lib.open_db
open_db.argtypes = [ctypes.c_char_p]
open_db.restype = None

begin_read = _lib.begin_read
begin_read.argtypes = []
begin_read.restype = c_txn

begin_write = _lib.begin_write
begin_write.argtypes = []
begin_write.restype = c_txn

commit = _lib.commit
commit.argtypes = [c_txn]
commit.restype = None

checkpoint = _lib.checkpoint
checkpoint.argtypes = []
checkpoint.restype = None

read_page = _lib.read_page
read_page.argtypes = [c_txn, ctypes.c_int]
read_page.restype = ctypes.POINTER(ctypes.c_ubyte * 4096)

write_page = _lib.write_page
write_page.argtypes = [c_txn, ctypes.c_int, ctypes.POINTER(ctypes.c_ubyte * 4096)]
write_page.restype = None

# ----------------------------
# Bind hash_join correctly — NOW USING size_t
# ----------------------------
_lib.hash_join.argtypes = [
    c_char_pp,              # const char** inner_rows
    ctypes.c_size_t,        # size_t inner_count
    c_char_pp,              # const char** outer_rows
    ctypes.c_size_t,        # size_t outer_count
    ctypes.c_char_p,        # const char* inner_key
    ctypes.c_char_p,        # const char* outer_key
    ctypes.POINTER(ctypes.c_char),  # char* output_buf
    ctypes.c_size_t         # size_t output_buf_size
]
_lib.hash_join.restype = ctypes.c_int

def hash_join_c(inner_rows: List[bytes], outer_rows: List[bytes],
                inner_key: str, outer_key: str, output_buf: bytearray) -> int:
    """Call C hash_join with proper ctypes conversion."""
    if not inner_rows or not outer_rows:
        return 0

    inner_arr = (ctypes.c_char_p * len(inner_rows))(*inner_rows)
    outer_arr = (ctypes.c_char_p * len(outer_rows))(*outer_rows)

    return _lib.hash_join(
        inner_arr,
        ctypes.c_size_t(len(inner_rows)),
        outer_arr,
        ctypes.c_size_t(len(outer_rows)),
        inner_key.encode('utf-8'),
        outer_key.encode('utf-8'),
        (ctypes.c_char * len(output_buf)).from_buffer(output_buf),
        ctypes.c_size_t(len(output_buf))
    )

# ----------------------------
# Data model
# ----------------------------
PAGE_SIZE = 4096

class DataType(Enum):
    INT = "INT"
    TEXT = "TEXT"

class Column:
    def __init__(self, name: str, dtype: DataType, primary_key: bool = False, unique: bool = False):
        self.name = name
        self.dtype = dtype
        self.primary_key = primary_key
        self.unique = unique

    def validate(self, value):
        if self.dtype == DataType.INT:
            if not isinstance(value, int):
                raise TypeError(f"Column '{self.name}' expects INT, got {type(value).__name__}")
        elif self.dtype == DataType.TEXT:
            if not isinstance(value, str):
                raise TypeError(f"Column '{self.name}' expects TEXT, got {type(value).__name__}")

class Table:
    def __init__(self, name: str, columns: List[Column], db_path: str, db: 'Database'):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.db_path = db_path
        self.db = db  # Reference to Database for page allocation
        self._pk_col = next((col.name for col in columns if col.primary_key), None)
        self._unique_cols = [col.name for col in columns if col.unique]
        self._pk_index = {}
        self._unique_indexes = {col: {} for col in self._unique_cols}
        self._rebuild_indexes()

    def _serialize_row(self, row: Dict[str, Any]) -> bytes:
        # Tag every row with its table name for isolation
        tagged_row = {"__table__": self.name, **row}
        return json.dumps(tagged_row).encode('utf-8')

    def _deserialize_row(self, data: bytes) -> Optional[Dict[str, Any]]:
        try:
            text = data.rstrip(b'\x00').decode('utf-8')
            if not text:
                return None
            row = json.loads(text)
            # Skip rows that don't belong to this table
            if row.get("__table__") != self.name:
                return None
            # Remove internal tag before returning
            row.pop("__table__", None)
            return row
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _rebuild_indexes(self):
        self._pk_index.clear()
        for idx in self._unique_indexes.values():
            idx.clear()

        txn = begin_read()
        page_id = 1
        while True:
            raw_ptr = read_page(txn, page_id)
            if not raw_ptr:
                break
            raw = bytes(raw_ptr.contents)
            if all(b == 0 for b in raw):
                break
            row = self._deserialize_row(raw)
            if row is None or row.get("__deleted__"):
                page_id += 1
                continue
            if self._pk_col and self._pk_col in row:
                self._pk_index[row[self._pk_col]] = page_id
            for col in self._unique_cols:
                if col in row:
                    self._unique_indexes[col][row[col]] = page_id
            page_id += 1

    def _find_page_by_key(self, col_name: str, value: Any) -> Optional[int]:
        if col_name == self._pk_col:
            return self._pk_index.get(value)
        if col_name in self._unique_indexes:
            return self._unique_indexes[col_name].get(value)
        return None

    def insert(self, row: Dict[str, Any]):
        if set(row.keys()) != set(self.columns.keys()):
            raise ValueError(f"Row must have exactly columns: {list(self.columns.keys())}")

        clean_row = {}
        for col_name, col in self.columns.items():
            val = row[col_name]
            col.validate(val)
            clean_row[col_name] = val

        if self._pk_col:
            pk_val = clean_row[self._pk_col]
            if pk_val in self._pk_index:
                raise ValueError(f"Duplicate primary key: {pk_val}")

        for col_name in self._unique_cols:
            uval = clean_row[col_name]
            if uval in self._unique_indexes[col_name]:
                raise ValueError(f"Duplicate unique value in '{col_name}': {uval}")

        txn = begin_write()
        try:
            row_bytes = self._serialize_row(clean_row)
            if len(row_bytes) > PAGE_SIZE:
                raise ValueError("Row too large")
            page_data = (ctypes.c_ubyte * PAGE_SIZE)()
            for i, b in enumerate(row_bytes):
                page_data[i] = b

            # Allocate page from global database allocator
            page_id = self.db.alloc_page()
            write_page(txn, page_id, ctypes.pointer(page_data))

            if self._pk_col:
                self._pk_index[pk_val] = page_id
            for col_name in self._unique_cols:
                self._unique_indexes[col_name][uval] = page_id

            commit(txn)

            if (page_id - 1) % 10 == 0:
                checkpoint()

        except Exception:
            raise

    def delete(self, key_col: str, key_val: Any):
        page_id = self._find_page_by_key(key_col, key_val)
        if page_id is None:
            raise KeyError(f"No row with {key_col} = {key_val}")

        txn = begin_write()
        try:
            tomb = self._serialize_row({"__deleted__": True})
            buf = (ctypes.c_ubyte * PAGE_SIZE)()
            for i, b in enumerate(tomb):
                buf[i] = b
            write_page(txn, page_id, ctypes.pointer(buf))

            if key_col == self._pk_col:
                del self._pk_index[key_val]
            elif key_col in self._unique_indexes:
                del self._unique_indexes[key_col][key_val]

            commit(txn)
        except Exception:
            raise

    def select(self, where_col: Optional[str] = None, where_val: Any = None) -> List[Dict[str, Any]]:
        rows = []
        txn = begin_read()
        page_id = 1
        while True:
            raw_ptr = read_page(txn, page_id)
            if not raw_ptr:
                break
            raw = bytes(raw_ptr.contents)
            if all(b == 0 for b in raw):
                break
            row = self._deserialize_row(raw)
            if row is None or row.get("__deleted__"):
                page_id += 1
                continue
            if where_col is not None:
                if row.get(where_col) != where_val:
                    page_id += 1
                    continue
            rows.append(row)
            page_id += 1
        return rows

    def hash_join(self, other: 'Table', self_key: str, other_key: str) -> List[Dict]:
        # Force join keys to STRINGS to ensure matching in C
        inner_rows = []
        for row in self.select():
            row_copy = row.copy()
            if self_key in row_copy:
                row_copy[self_key] = str(row_copy[self_key])
            inner_rows.append(json.dumps(row_copy).encode('utf-8'))

        outer_rows = []
        for row in other.select():
            row_copy = row.copy()
            if other_key in row_copy:
                row_copy[other_key] = str(row_copy[other_key])
            outer_rows.append(json.dumps(row_copy).encode('utf-8'))

        # >>>>>>>>>> ADDITIONAL DEBUG CHECKS <<<<<<<<<<
        if not inner_rows:
            print("[JOIN] Warning: inner_rows is empty!")
        if not outer_rows:
            print("[JOIN] Warning: outer_rows is empty!")

        print(f"[JOIN DEBUG] About to call C hash_join with:")
        print(f"  inner_rows = {len(inner_rows)} rows")
        print(f"  outer_rows = {len(outer_rows)} rows")
        print(f"  inner_key = '{self_key}', outer_key = '{other_key}'")

        if not inner_rows or not outer_rows:
            return []

        # >>>>>>>>>> DEBUG PRINTS ADDED HERE <<<<<<<<<<
        print(f"\n[DEBUG] Hash join plan:")
        print(f"  Building hash table from table: '{self.name}' using key: '{self_key}'")
        print(f"  Probing against table: '{other.name}' using key: '{other_key}'")
        print(f"  Inner rows ({len(inner_rows)}):")
        for r in inner_rows:
            print(f"    {r.decode('utf-8')}")
        print(f"  Outer rows ({len(outer_rows)}):")
        for r in outer_rows:
            print(f"    {r.decode('utf-8')}")
        print()  # blank line for clarity
        # >>>>>>>>>> END DEBUG <<<<<<<<<<

        output_buf = bytearray(1024 * 1024)  # 1MB buffer
        count = hash_join_c(inner_rows, outer_rows, self_key, other_key, output_buf)

        results = []
        pos = 0
        for _ in range(count):
            try:
                end = pos
                while end < len(output_buf) and output_buf[end] != 0:
                    end += 1
                if end <= pos:
                    break
                segment = output_buf[pos:end]
                if segment:
                    row = json.loads(segment.decode('utf-8'))
                    results.append(row)
                pos = end + 1
            except Exception:
                break

        return results


class Database:
    CATALOG_PAGE = 0
    NEXT_PAGE_KEY = "next_page"  # Key for storing global next_page in catalog

    def __init__(self, path: str):
        open_db(path.encode('utf-8'))
        self.path = path
        self.tables = {}
        self.next_page = 1  # Global page allocator
        self._load_catalog()

    def alloc_page(self) -> int:
        """Allocate a new page ID globally."""
        page = self.next_page
        self.next_page += 1
        return page

    def _save_catalog(self):
        catalog = {
            "tables": {
                name: [
                    (col.name, col.dtype.value, col.primary_key, col.unique)
                    for col in table.columns.values()
                ]
                for name, table in self.tables.items()
            },
            # Save global next_page counter
            self.NEXT_PAGE_KEY: self.next_page
        }
        txn = begin_write()
        try:
            data = json.dumps(catalog).encode('utf-8')
            buf = (ctypes.c_ubyte * PAGE_SIZE)()
            for i, b in enumerate(data):
                buf[i] = b
            write_page(txn, self.CATALOG_PAGE, ctypes.pointer(buf))
            commit(txn)
        except Exception as e:
            print(f"Warning: failed to save catalog: {e}")

    def _load_catalog(self):
        txn = begin_read()
        raw_ptr = read_page(txn, self.CATALOG_PAGE)
        if not raw_ptr:
            return
        raw = bytes(raw_ptr.contents)
        try:
            text = raw.rstrip(b'\x00').decode('utf-8')
            if text:
                catalog = json.loads(text)
                # Restore global next_page counter
                self.next_page = catalog.get(self.NEXT_PAGE_KEY, 1)

                for name, col_specs in catalog.get("tables", {}).items():
                    columns = []
                    for spec in col_specs:
                        col_name, dtype_str, pk, uniq = spec
                        dtype = DataType(dtype_str)
                        columns.append(Column(col_name, dtype, primary_key=pk, unique=uniq))
                    # Pass self (Database instance) to Table
                    self.tables[name] = Table(name, columns, self.path, self)
        except:
            pass

    def create_table(self, name: str, columns: List[Column]) -> Table:
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
        pk_count = sum(1 for col in columns if col.primary_key)
        if pk_count > 1:
            raise ValueError("Only one primary key allowed")
        # Pass self to Table so it can allocate pages
        tbl = Table(name, columns, self.path, self)
        self.tables[name] = tbl
        self._save_catalog()
        return tbl

    def get_table(self, name: str) -> Table:
        if name not in self.tables:
            raise KeyError(f"Table '{name}' does not exist")
        return self.tables[name]