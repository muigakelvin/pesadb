# waldb.py
import ctypes
import ctypes.util
import os
from typing import List

_lib = ctypes.CDLL(os.path.join(os.path.dirname(__file__), 'libwaldb.so'))

PAGE_SIZE = 4096

class WriteTxn(ctypes.Structure):
    _fields_ = [("id", ctypes.c_uint32)]

class ReaderTxn(ctypes.Structure):
    _fields_ = [("snapshot", ctypes.c_uint64)]

_lib.waldb_open.argtypes = [ctypes.c_char_p]
_lib.waldb_open.restype = None

_lib.waldb_close.argtypes = []
_lib.waldb_close.restype = None

_lib.waldb_begin_write.argtypes = []
_lib.waldb_begin_write.restype = WriteTxn

_lib.waldb_begin_read.argtypes = []
_lib.waldb_begin_read.restype = ReaderTxn

_lib.waldb_write_page.argtypes = [
    ctypes.POINTER(WriteTxn),
    ctypes.c_uint32,
    ctypes.c_void_p
]
_lib.waldb_write_page.restype = None

_lib.waldb_read_page.argtypes = [
    ctypes.POINTER(ReaderTxn),
    ctypes.c_uint32,
    ctypes.c_void_p
]
_lib.waldb_read_page.restype = None

_lib.waldb_commit.argtypes = [ctypes.POINTER(WriteTxn)]
_lib.waldb_commit.restype = None

_lib.waldb_checkpoint.argtypes = []
_lib.waldb_checkpoint.restype = None

_lib.hash_join.argtypes = [
    ctypes.POINTER(ctypes.POINTER(ctypes.c_char)),
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.POINTER(ctypes.c_char)),
    ctypes.c_size_t,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_size_t
]
_lib.hash_join.restype = ctypes.c_int

def open_db(path: str):
    _lib.waldb_open(path.encode())

def close_db():
    _lib.waldb_close()

def begin_write() -> WriteTxn:
    return _lib.waldb_begin_write()

def begin_read() -> ReaderTxn:
    return _lib.waldb_begin_read()

def write_page(txn: WriteTxn, page_id: int, data: bytes):
    if len(data) != PAGE_SIZE:
        raise ValueError(f"Data must be {PAGE_SIZE} bytes")
    buf = ctypes.create_string_buffer(data)
    _lib.waldb_write_page(ctypes.byref(txn), page_id, buf)

def read_page(txn: ReaderTxn, page_id: int) -> bytes:
    buf = ctypes.create_string_buffer(PAGE_SIZE)
    _lib.waldb_read_page(ctypes.byref(txn), page_id, buf)
    return buf.raw

def commit(txn: WriteTxn):
    _lib.waldb_commit(ctypes.byref(txn))

def checkpoint():
    _lib.waldb_checkpoint()

def hash_join(
    inner_pages: List[bytes],
    outer_pages: List[bytes],
    inner_key: str,
    outer_key: str,
    output_buffer: bytearray
) -> int:
    inner_arr = (ctypes.POINTER(ctypes.c_char) * len(inner_pages))()
    for i, p in enumerate(inner_pages):
        inner_arr[i] = ctypes.cast(ctypes.create_string_buffer(p), ctypes.POINTER(ctypes.c_char))

    outer_arr = (ctypes.POINTER(ctypes.c_char) * len(outer_pages))()
    for i, p in enumerate(outer_pages):
        outer_arr[i] = ctypes.cast(ctypes.create_string_buffer(p), ctypes.POINTER(ctypes.c_char))

    buf_ptr = ctypes.cast(output_buffer, ctypes.c_char_p)
    return _lib.hash_join(
        inner_arr, len(inner_pages),
        outer_arr, len(outer_pages),
        inner_key.encode(),
        outer_key.encode(),
        buf_ptr,
        len(output_buffer)
    )