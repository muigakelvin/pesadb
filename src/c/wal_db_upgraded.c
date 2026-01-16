// wal_db_upgraded.c
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <limits.h>

#define PAGE_SIZE 4096
#define MAX_TX 1024
#define MAX_READERS 32
#define CACHE_SIZE 64
#define WAL_MAGIC_COMMIT 0xC0DECAFE

/* ================= WAL TYPES ================= */
typedef enum {
    WAL_PAGE   = 1,
    WAL_COMMIT = 2
} WalRecordType;

typedef struct {
    uint32_t type;
    uint32_t tx_id;
    uint32_t page_id;
    uint8_t  data[PAGE_SIZE];
} WalPageRecord;

typedef struct {
    uint32_t type;
    uint32_t tx_id;
    uint32_t magic;
} WalCommitRecord;

/* ================== PAGE CACHE ================== */
typedef struct {
    uint32_t page_id;
    uint32_t owner_tx;
    bool dirty;
    uint8_t data[PAGE_SIZE];
} CachedPage;

/* ============== GLOBAL STATE ============== */
static int db_fd = -1;
static int wal_fd = -1;

static uint32_t next_tx_id = 1;
static uint64_t reader_snapshots[MAX_READERS] = {0};
static size_t reader_count = 0;

static CachedPage cache[CACHE_SIZE];
static size_t cache_count = 0;

/* ================= FILE HANDLING ================= */
static void open_database(const char *name) {
    db_fd = open(name, O_RDWR | O_CREAT, 0644);
    if (db_fd < 0) { perror("open db"); exit(1); }

    char wal_name[256];
    snprintf(wal_name, sizeof(wal_name), "%s-wal", name);
    wal_fd = open(wal_name, O_RDWR | O_CREAT, 0644);
    if (wal_fd < 0) { perror("open wal"); exit(1); }
}

/* ================= TRANSACTIONS ================= */
typedef struct {
    uint32_t tx_id;
} WriteTxn;

static WriteTxn begin_write_txn() {
    WriteTxn tx;
    tx.tx_id = next_tx_id++;
    return tx;
}

typedef struct {
    uint64_t snapshot;
} ReaderTxn;

static ReaderTxn begin_read_txn() {
    ReaderTxn r;
    r.snapshot = lseek(wal_fd, 0, SEEK_END);
    if (reader_count < MAX_READERS) {
        reader_snapshots[reader_count++] = r.snapshot;
    }
    return r;
}

/* ================= PAGE CACHE FUNCTIONS ================= */
static CachedPage* find_cached_page(uint32_t page_id) {
    for (size_t i = 0; i < cache_count; i++) {
        if (cache[i].page_id == page_id)
            return &cache[i];
    }
    return NULL;
}

static CachedPage* get_or_create_cached_page(uint32_t page_id, uint32_t tx_id) {
    CachedPage* cp = find_cached_page(page_id);
    if (cp) return cp;

    if (cache_count >= CACHE_SIZE) {
        fprintf(stderr, "Cache full!\n");
        exit(1);
    }

    cp = &cache[cache_count++];
    cp->page_id = page_id;
    cp->owner_tx = tx_id;
    cp->dirty = false;
    memset(cp->data, 0, PAGE_SIZE);
    return cp;
}

/* ================= WAL FUNCTIONS ================= */
static void wal_append_page(WriteTxn *tx, uint32_t page_id, void *data) {
    WalPageRecord rec;
    rec.type = WAL_PAGE;
    rec.tx_id = tx->tx_id;
    rec.page_id = page_id;
    memcpy(rec.data, data, PAGE_SIZE);
    if (write(wal_fd, &rec, sizeof(rec)) != sizeof(rec)) {
        perror("write wal page");
        exit(1);
    }
}

static void wal_commit(WriteTxn *tx) {
    WalCommitRecord rec;
    rec.type = WAL_COMMIT;
    rec.tx_id = tx->tx_id;
    rec.magic = WAL_MAGIC_COMMIT;
    if (write(wal_fd, &rec, sizeof(rec)) != sizeof(rec)) {
        perror("write wal commit");
        exit(1);
    }
    fsync(wal_fd);
}

/* ================= DB IO ================= */
static void read_page_from_db(uint32_t page_id, void *out) {
    lseek(db_fd, (off_t)page_id * PAGE_SIZE, SEEK_SET);
    ssize_t n = read(db_fd, out, PAGE_SIZE);
    if (n <= 0) {
        memset(out, 0, PAGE_SIZE);
    }
}

static void write_page_to_db(uint32_t page_id, void *data) {
    lseek(db_fd, (off_t)page_id * PAGE_SIZE, SEEK_SET);
    if (write(db_fd, data, PAGE_SIZE) != PAGE_SIZE) {
        perror("write db page");
        exit(1);
    }
}

/* ================= WAL LOOKUP ================= */
static bool wal_read_page(uint32_t page_id, uint64_t snapshot, void *out) {
    off_t pos = 0;
    bool committed[MAX_TX] = {false};

    lseek(wal_fd, 0, SEEK_SET);
    while (pos < (off_t)snapshot) {
        uint32_t type;
        ssize_t n = read(wal_fd, &type, sizeof(type));
        if (n <= 0) break;

        if (type == WAL_COMMIT) {
            WalCommitRecord cr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &cr, sizeof(cr));
            if (cr.magic == WAL_MAGIC_COMMIT && cr.tx_id < MAX_TX) {
                committed[cr.tx_id] = true;
            }
            pos += sizeof(cr);
        } else {
            WalPageRecord pr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &pr, sizeof(pr));
            pos += sizeof(pr);
        }
    }

    pos = snapshot;
    while (pos > (off_t)sizeof(WalPageRecord)) {
        pos -= sizeof(WalPageRecord);
        lseek(wal_fd, pos, SEEK_SET);

        WalPageRecord pr;
        if (read(wal_fd, &pr, sizeof(pr)) <= 0) break;

        if (pr.type == WAL_PAGE &&
            pr.page_id == page_id &&
            pr.tx_id < MAX_TX &&
            committed[pr.tx_id]) {
            memcpy(out, pr.data, PAGE_SIZE);
            return true;
        }
    }

    return false;
}

/* ================= HIGH-LEVEL API ================= */
static void write_page_int(WriteTxn *tx, uint32_t page_id, void *data) {
    CachedPage* cp = get_or_create_cached_page(page_id, tx->tx_id);
    memcpy(cp->data, data, PAGE_SIZE);
    cp->dirty = true;
    cp->owner_tx = tx->tx_id;
}

static void commit_tx(WriteTxn *tx) {
    for (size_t i = 0; i < cache_count; i++) {
        if (cache[i].dirty && cache[i].owner_tx == tx->tx_id) {
            wal_append_page(tx, cache[i].page_id, cache[i].data);
            cache[i].dirty = false;
        }
    }
    wal_commit(tx);
}

/* ================= READ API ================= */
static void read_page_int(ReaderTxn *rx, uint32_t page_id, void *out) {
    CachedPage* cp = find_cached_page(page_id);
    if (cp) {
        memcpy(out, cp->data, PAGE_SIZE);
        return;
    }

    if (!wal_read_page(page_id, rx->snapshot, out)) {
        read_page_from_db(page_id, out);
    }
}

/* ================= CHECKPOINT ================= */
static uint64_t oldest_reader_snapshot() {
    if (reader_count == 0) return 0;
    uint64_t min = ULLONG_MAX;
    for (size_t i = 0; i < reader_count; i++) {
        if (reader_snapshots[i] < min) min = reader_snapshots[i];
    }
    return min;
}

static void checkpoint_int() {
    uint64_t safe = oldest_reader_snapshot();
    off_t pos = 0;
    lseek(wal_fd, 0, SEEK_SET);

    while (pos < (off_t)safe) {
        uint32_t type;
        if (read(wal_fd, &type, sizeof(type)) <= 0) break;

        if (type == WAL_PAGE) {
            WalPageRecord pr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &pr, sizeof(pr));
            write_page_to_db(pr.page_id, pr.data);
            pos += sizeof(pr);
        } else {
            WalCommitRecord cr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &cr, sizeof(cr));
            pos += sizeof(cr);
        }
    }

    fsync(db_fd);
}

/* ================= RECOVERY ================= */
static void wal_recover() {
    bool committed[MAX_TX] = {false};
    lseek(wal_fd, 0, SEEK_SET);

    while (true) {
        uint32_t type;
        if (read(wal_fd, &type, sizeof(type)) <= 0) break;

        if (type == WAL_COMMIT) {
            WalCommitRecord cr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &cr, sizeof(cr));
            if (cr.magic == WAL_MAGIC_COMMIT && cr.tx_id < MAX_TX) {
                committed[cr.tx_id] = true;
            }
        } else {
            WalPageRecord pr;
            lseek(wal_fd, -(off_t)sizeof(uint32_t), SEEK_CUR);
            read(wal_fd, &pr, sizeof(pr));
            if (pr.tx_id < MAX_TX && committed[pr.tx_id]) {
                write_page_to_db(pr.page_id, pr.data);
            }
        }
    }

    fsync(db_fd);
}

/* =============== PUBLIC API WRAPPERS =============== */
void waldb_open(const char* path) {
    static bool opened = false;
    if (opened) return;
    open_database(path);
    wal_recover();
    opened = true;
}

void waldb_close(void) {
    if (db_fd >= 0) close(db_fd);
    if (wal_fd >= 0) close(wal_fd);
}

WriteTxn waldb_begin_write(void) {
    return begin_write_txn();
}

ReaderTxn waldb_begin_read(void) {
    return begin_read_txn();
}

void waldb_write_page(WriteTxn* txn, uint32_t page_id, const void* data) {
    write_page_int(txn, page_id, (void*)data);
}

void waldb_read_page(ReaderTxn* txn, uint32_t page_id, void* buffer) {
    read_page_int(txn, page_id, buffer);
}

void waldb_commit(WriteTxn* txn) {
    commit_tx(txn);
}

void waldb_checkpoint(void) {
    checkpoint_int();
}

/* =============== PYTHON-FRIENDLY EXPORTS =============== */
// These match the names used in executor.py

void open_db(const char* path) {
    waldb_open(path);
}

void* begin_read(void) {
    ReaderTxn* txn = malloc(sizeof(ReaderTxn));
    *txn = waldb_begin_read();
    return txn;
}

void* begin_write(void) {
    WriteTxn* txn = malloc(sizeof(WriteTxn));
    *txn = waldb_begin_write();
    return txn;
}

void commit(void* txn_ptr) {
    if (!txn_ptr) return;
    WriteTxn* txn = (WriteTxn*)txn_ptr;
    waldb_commit(txn);
    free(txn);
}

void checkpoint(void) {
    waldb_checkpoint();
}

unsigned char (*read_page(void* txn_ptr, int page_id))[4096] {
    static unsigned char buffer[4096];
    memset(buffer, 0, sizeof(buffer));
    if (!txn_ptr) {
        return &buffer;
    }
    ReaderTxn* txn = (ReaderTxn*)txn_ptr;
    waldb_read_page(txn, (uint32_t)page_id, buffer);
    return &buffer;
}

void write_page(void* txn_ptr, int page_id, unsigned char (*data)[4096]) {
    if (!txn_ptr || !data) return;
    WriteTxn* txn = (WriteTxn*)txn_ptr;
    waldb_write_page(txn, (uint32_t)page_id, *data);
}