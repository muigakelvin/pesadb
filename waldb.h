#ifndef WALDB_H
#define WALDB_H

#include <stdint.h>
#include <stddef.h>   // ← ADD THIS (for size_t, NULL)
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Transaction types
typedef struct { uint32_t id; } WriteTxn;
typedef struct { uint64_t snapshot; } ReaderTxn;

// Core WAL API
void waldb_open(const char* path);
void waldb_close(void);
WriteTxn waldb_begin_write(void);
ReaderTxn waldb_begin_read(void);
void waldb_write_page(WriteTxn* txn, uint32_t page_id, const void* data);
void waldb_read_page(ReaderTxn* txn, uint32_t page_id, void* buffer);
void waldb_commit(WriteTxn* txn);
void waldb_checkpoint(void);

// Hash join declaration (only declaration!)
int hash_join(
    const char* inner_pages[], size_t inner_count,
    const char* outer_pages[], size_t outer_count,
    const char* inner_key_name,
    const char* outer_key_name,
    char* output_buffer,
    size_t max_output_size
);

#ifdef __cplusplus
}
#endif

#endif