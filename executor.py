# executor.py
import pickle
from typing import List, Dict, Any
from waldb import *

class Table:
    def __init__(self, name: str):
        self.name = name
        self._next_page = 1

    def insert(self, row: Dict[str, Any]):
        txn = begin_write()
        try:
            row_bytes = pickle.dumps(row)
            if len(row_bytes) > 4096:
                raise ValueError("Row too large")
            page_data = bytearray(4096)
            page_data[:len(row_bytes)] = row_bytes
            write_page(txn, self._next_page, bytes(page_data))
            self._next_page += 1
            commit(txn)
        except Exception:
            raise

    def delete(self, key_col: str, key_val: Any):
        """Mark matching rows as deleted using a tombstone."""
        txn = begin_write()
        try:
            read_txn = begin_read()
            page_id = 1
            while True:
                try:
                    raw = read_page(read_txn, page_id)
                    if all(b == 0 for b in raw):
                        break
                    row = pickle.loads(raw.rstrip(b'\x00'))
                    # Skip already deleted rows
                    if row.get("__deleted__"):
                        page_id += 1
                        continue
                    # Check if this row matches the deletion condition
                    if row.get(key_col) == key_val:
                        # Write tombstone
                        tomb = pickle.dumps({"__deleted__": True})
                        buf = bytearray(4096)
                        buf[:len(tomb)] = tomb
                        write_page(txn, page_id, bytes(buf))
                    page_id += 1
                except:
                    page_id += 1
                    continue
            commit(txn)
        except Exception:
            raise

    def select(self) -> List[Dict[str, Any]]:
        rows = []
        txn = begin_read()
        page_id = 1
        while True:
            try:
                raw = read_page(txn, page_id)
                if all(b == 0 for b in raw):
                    break
                row = pickle.loads(raw.rstrip(b'\x00'))
                # ✅ Skip deleted rows
                if not row.get("__deleted__"):
                    rows.append(row)
                page_id += 1
            except:
                break
        return rows

    def hash_join(self, other: 'Table', self_key: str, other_key: str) -> List[Dict]:
        inner_txn = begin_read()
        outer_txn = begin_read()

        inner_pages = []
        page_id = 1
        while True:
            try:
                raw = read_page(inner_txn, page_id)
                if all(b == 0 for b in raw):
                    break
                row = pickle.loads(raw.rstrip(b'\x00'))
                # ✅ Skip deleted rows in inner table
                if not row.get("__deleted__"):
                    inner_pages.append(raw)
                page_id += 1
            except:
                break

        outer_pages = []
        page_id = 1
        while True:
            try:
                raw = read_page(outer_txn, page_id)
                if all(b == 0 for b in raw):
                    break
                row = pickle.loads(raw.rstrip(b'\x00'))
                # ✅ Skip deleted rows in outer table
                if not row.get("__deleted__"):
                    outer_pages.append(raw)
                page_id += 1
            except:
                break

        output_buf = bytearray(1024 * 1024)
        count = hash_join(inner_pages, outer_pages, self_key, other_key, output_buf)

        results = []
        pos = 0
        for _ in range(count):
            try:
                end = pos
                while end < len(output_buf) and output_buf[end] != 0:
                    end += 1
                if end == pos:
                    break
                row = pickle.loads(output_buf[pos:end])
                # ✅ Also skip tombstones in joined result (defensive)
                if not row.get("__deleted__"):
                    results.append(row)
                pos = end + 1
            except:
                break

        return results

class Database:
    def __init__(self, path: str):
        open_db(path)
        self.tables = {}

    def create_table(self, name: str, columns: List[str]) -> Table:
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
        tbl = Table(name)
        self.tables[name] = tbl
        return tbl

    def get_table(self, name: str) -> Table:
        return self.tables[name]