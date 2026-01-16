#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>  // ← ADD THIS LINE

#define HASH_SIZE 4096
#define INIT_CAP 4

/* =========================
   Hash table entry
   ========================= */
typedef struct {
    char* key;
    char** rows;
    size_t count;
    size_t cap;
} HashEntry;

/* =========================
   Utilities
   ========================= */
static unsigned long hash_str(const char* s) {
    unsigned long h = 5381;
    int c;
    while ((c = *s++))
        h = ((h << 5) + h) + c;
    return h;
}

static char* strdup_safe(const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char* d = malloc(len);
    if (d) memcpy(d, s, len);
    return d;
}

/* =========================
   Hash Join
   ========================= */
int hash_join(
    const char** inner_rows,
    int inner_count,
    const char** outer_rows,
    int outer_count,
    const char* inner_key,
    const char* outer_key,
    char* output_buf,
    int output_buf_size
) {
    int result_count = 0;
    int out_pos = 0;
    PyObject* json = NULL;
    PyObject* loads = NULL;
    PyObject* dumps = NULL;

    /* ---------- Python setup ---------- */
    PyGILState_STATE gstate = PyGILState_Ensure();

    json = PyImport_ImportModule("json");
    if (!json) goto cleanup;

    loads = PyObject_GetAttrString(json, "loads");
    dumps = PyObject_GetAttrString(json, "dumps");

    if (!loads || !dumps) goto cleanup;

    /* ---------- Build hash table ---------- */
    HashEntry table[HASH_SIZE];
    memset(table, 0, sizeof(table));

    for (int i = 0; i < inner_count; i++) {
        PyObject* py_row = PyUnicode_FromString(inner_rows[i]);
        PyObject* row_dict = PyObject_CallOneArg(loads, py_row);
        Py_DECREF(py_row);

        if (!row_dict) {
            fprintf(stderr, "[BUILD] Failed to parse inner row: %s\n", inner_rows[i]);
            continue;
        }

        PyObject* key_obj = PyDict_GetItemString(row_dict, inner_key);
        if (!key_obj) {
            fprintf(stderr, "[BUILD] Key '%s' not found in row: %s\n", inner_key, inner_rows[i]);
            Py_DECREF(row_dict);
            continue;
        }

        PyObject* key_str = PyObject_Str(key_obj);
        const char* key_cstr = PyUnicode_AsUTF8(key_str);
        if (!key_cstr) {
            fprintf(stderr, "[BUILD] Failed to convert key to string\n");
            Py_DECREF(key_str);
            Py_DECREF(row_dict);
            continue;
        }

        unsigned long idx = hash_str(key_cstr) % HASH_SIZE;
        unsigned long start = idx;

        // Linear probing to find slot
        while (table[idx].key != NULL) {
            if (strcmp(table[idx].key, key_cstr) == 0) {
                break;
            }
            idx = (idx + 1) % HASH_SIZE;
            if (idx == start) {
                fprintf(stderr, "[BUILD] Hash table full!\n");
                Py_DECREF(key_str);
                Py_DECREF(row_dict);
                goto cleanup;
            }
        }

        if (table[idx].key == NULL) {
            table[idx].key = strdup_safe(key_cstr);
            table[idx].rows = malloc(sizeof(char*) * INIT_CAP);
            table[idx].cap = INIT_CAP;
            table[idx].count = 0;
        }

        if (table[idx].count == table[idx].cap) {
            table[idx].cap *= 2;
            table[idx].rows = realloc(
                table[idx].rows,
                sizeof(char*) * table[idx].cap
            );
        }

        table[idx].rows[table[idx].count++] =
            strdup_safe(inner_rows[i]);

        fprintf(stderr, "[BUILD] Stored key='%s' (row: %s)\n", key_cstr, inner_rows[i]);

        Py_DECREF(key_str);
        Py_DECREF(row_dict);
    }

    /* ---------- Probe phase ---------- */
    for (int i = 0; i < outer_count; i++) {
        PyObject* py_row = PyUnicode_FromString(outer_rows[i]);
        PyObject* outer_dict = PyObject_CallOneArg(loads, py_row);
        Py_DECREF(py_row);

        if (!outer_dict) {
            fprintf(stderr, "[PROBE] Failed to parse outer row: %s\n", outer_rows[i]);
            continue;
        }

        PyObject* key_obj = PyDict_GetItemString(outer_dict, outer_key);
        if (!key_obj) {
            fprintf(stderr, "[PROBE] Key '%s' not found in row: %s\n", outer_key, outer_rows[i]);
            Py_DECREF(outer_dict);
            continue;
        }

        PyObject* key_str = PyObject_Str(key_obj);
        const char* key_cstr = PyUnicode_AsUTF8(key_str);
        if (!key_cstr) {
            fprintf(stderr, "[PROBE] Failed to convert key to string\n");
            Py_DECREF(key_str);
            Py_DECREF(outer_dict);
            continue;
        }

        unsigned long idx = hash_str(key_cstr) % HASH_SIZE;
        unsigned long start = idx;

        fprintf(stderr, "[PROBE] Looking for key='%s' (hash=%lu)\n", key_cstr, idx);

        // Linear probing to find matching key
        bool found = false;
        while (table[idx].key != NULL) {
            if (strcmp(table[idx].key, key_cstr) == 0) {
                fprintf(stderr, "[MATCH] Found %zu rows for key='%s'\n", table[idx].count, key_cstr);
                for (size_t j = 0; j < table[idx].count; j++) {
                    PyObject* inner_py = PyUnicode_FromString(table[idx].rows[j]);
                    PyObject* inner_dict = PyObject_CallOneArg(loads, inner_py);
                    Py_DECREF(inner_py);

                    if (!inner_dict) continue;

                    PyObject* merged = PyDict_Copy(inner_dict);
                    PyDict_Update(merged, outer_dict);

                    PyObject* dumped = PyObject_CallOneArg(dumps, merged);
                    if (dumped) {
                        const char* out = PyUnicode_AsUTF8(dumped);
                        if (out) {
                            int len = strlen(out) + 1;
                            if (out_pos + len <= output_buf_size) {
                                memcpy(output_buf + out_pos, out, len);
                                out_pos += len;
                                result_count++;
                                fprintf(stderr, "[OUTPUT] Wrote: %s\n", out);
                            }
                        }
                        Py_DECREF(dumped);
                    }

                    Py_DECREF(merged);
                    Py_DECREF(inner_dict);
                }
                found = true;
                break;
            }
            idx = (idx + 1) % HASH_SIZE;
            if (idx == start) break;
        }

        if (!found) {
            fprintf(stderr, "[PROBE] No match for key='%s'\n", key_cstr);
        }

        Py_DECREF(key_str);
        Py_DECREF(outer_dict);
    }

cleanup:
    /* ---------- Cleanup hash table ---------- */
    for (int i = 0; i < HASH_SIZE; i++) {
        if (table[i].key) {
            free(table[i].key);
            for (size_t j = 0; j < table[i].count; j++) {
                free(table[i].rows[j]);
            }
            free(table[i].rows);
        }
    }

    Py_XDECREF(loads);
    Py_XDECREF(dumps);
    Py_XDECREF(json);

    PyGILState_Release(gstate);
    return result_count;
}