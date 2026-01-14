#include "waldb.h"
#include <Python.h>
#include <string.h>
#include <stdlib.h>

#define PAGE_SIZE 4096
#define HASH_TABLE_SIZE 1024

static PyObject* unpickle_page(const char* page_data) {
    if (page_data[0] == 0) return NULL;

    PyObject* pickle_module = PyImport_ImportModule("pickle");
    if (!pickle_module) return NULL;

    PyObject* loads = PyObject_GetAttrString(pickle_module, "loads");
    Py_DECREF(pickle_module);
    if (!loads) return NULL;

    PyObject* args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, PyBytes_FromStringAndSize(page_data, PAGE_SIZE));

    PyObject* result = PyObject_CallObject(loads, args);
    Py_DECREF(loads);
    Py_DECREF(args);
    return result;
}

static int64_t get_dict_int(PyObject* dict, const char* key) {
    if (!PyDict_Check(dict)) return -1;
    PyObject* val = PyDict_GetItemString(dict, key);
    if (!val || !PyLong_Check(val)) return -1;
    return PyLong_AsLongLong(val);
}

int hash_join(
    const char* inner_pages[], size_t inner_count,
    const char* outer_pages[], size_t outer_count,
    const char* inner_key_name,
    const char* outer_key_name,
    char* output_buffer,
    size_t max_output_size
) {
    if (!Py_IsInitialized()) Py_Initialize();

    typedef struct {
        int64_t key;
        PyObject* row;
    } HashEntry;

    HashEntry table[HASH_TABLE_SIZE] = {{0}};

    // Build phase
    for (size_t i = 0; i < inner_count; i++) {
        PyObject* row = unpickle_page(inner_pages[i]);
        if (!row) continue;

        int64_t key = get_dict_int(row, inner_key_name);
        if (key == -1) { Py_DECREF(row); continue; }

        size_t idx = key % HASH_TABLE_SIZE;
        while (table[idx].row && table[idx].key != key) {
            idx = (idx + 1) % HASH_TABLE_SIZE;
        }
        if (!table[idx].row) {
            table[idx].key = key;
            table[idx].row = row;
        } else {
            Py_DECREF(row);
        }
    }

    // Probe phase
    size_t out_pos = 0;
    int result_count = 0;

    for (size_t i = 0; i < outer_count; i++) {
        PyObject* outer_row = unpickle_page(outer_pages[i]);
        if (!outer_row) continue;

        int64_t key = get_dict_int(outer_row, outer_key_name);
        if (key == -1) { Py_DECREF(outer_row); continue; }

        size_t idx = key % HASH_TABLE_SIZE;
        while (table[idx].row && table[idx].key != key) {
            idx = (idx + 1) % HASH_TABLE_SIZE;
        }

        if (table[idx].row) {
            PyObject* merged = PyDict_Copy(table[idx].row);
            PyDict_Update(merged, outer_row);

            PyObject* pickle_module = PyImport_ImportModule("pickle");
            PyObject* dumps = PyObject_GetAttrString(pickle_module, "dumps");
            PyObject* args = PyTuple_New(1);
            PyTuple_SetItem(args, 0, merged);
            PyObject* pickled = PyObject_CallObject(dumps, args);

            if (pickled && PyBytes_Check(pickled)) {
                Py_ssize_t len;
                char* data;
                PyBytes_AsStringAndSize(pickled, &data, &len);
                if (out_pos + len <= max_output_size) {
                    memcpy(output_buffer + out_pos, data, len);
                    out_pos += len;
                    result_count++;
                }
            }

            Py_XDECREF(merged);
            Py_XDECREF(pickled);
            Py_DECREF(args);
            Py_DECREF(dumps);
            Py_DECREF(pickle_module);
        }

        Py_DECREF(outer_row);
    }

    // Cleanup
    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        if (table[i].row) Py_DECREF(table[i].row);
    }

    return result_count;
}
