#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

#include "rocksdb/c.h"
#include "db.h"
#include "../destor.h"

static rocksdb_t *dbList[DB_ALL];
static pthread_mutex_t dbLock[DB_ALL];
static rocksdb_readoptions_t *readoptions = NULL;
static rocksdb_writeoptions_t *writeoptions = NULL;

void init_RocksDB(int index) {
    pthread_mutex_init(&dbLock[index], NULL);
    // generate options
    readoptions = rocksdb_readoptions_create();
    writeoptions = rocksdb_writeoptions_create();

    // generate path
    char path[1024];
    snprintf(path, 1024, "%s/rocksdb%d", destor.working_directory, index);

    char *err = NULL;
    rocksdb_options_t *options = rocksdb_options_create();
    // set block
    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();
    // rocksdb_block_based_options_set_block_size(table_options, 64 * 1024);
    rocksdb_block_based_options_set_no_block_cache(table_options, 1);
    rocksdb_options_set_block_based_table_factory(options, table_options);
    if (destor.direct_reads) {
        rocksdb_options_set_use_direct_reads(options, 1);
    }
    rocksdb_options_set_enable_blob_files(options, 1);
    rocksdb_options_set_min_blob_size(options, 1024);
    rocksdb_options_set_max_open_files(options, 4000);

    rocksdb_options_set_compression(options, rocksdb_no_compression);
    rocksdb_options_set_write_buffer_size(options, 1024 * 1024);

    // Optimize RocksDB. This is the easiest way to
    // get RocksDB to perform well.
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    // Set # of online cores
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    // rocksdb_options_optimize_level_style_compaction(options, 0);
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);
    dbList[index] = rocksdb_open(options, path, &err);
    assert(!err);
    rocksdb_options_destroy(options);
    rocksdb_block_based_options_destroy(table_options);
}

void close_RocksDB(int index) {
    rocksdb_close(dbList[index]);
    pthread_mutex_destroy(&dbLock[index]);
    if (readoptions) {
        rocksdb_readoptions_destroy(readoptions);
        readoptions = NULL;
    }
    if (writeoptions) {
        rocksdb_writeoptions_destroy(writeoptions);
        writeoptions = NULL;
    }
}

void put_RocksDB(int index, char *key, size_t keySize, char *value, size_t valueSize) {
    pthread_mutex_lock(&dbLock[index]);
    char *err = NULL;
    rocksdb_put(dbList[index], writeoptions, key, keySize, value, valueSize, &err);
    if (err) {
        fprintf(stderr, "rocksdb_put error: %s\n", err);
        assert(0);
    }
    pthread_mutex_unlock(&dbLock[index]);
}

void get_RocksDB(int index, char *key, size_t keySize, char **value, size_t *valueSize) {
    pthread_mutex_lock(&dbLock[index]);
    char *err = NULL;
    *value = rocksdb_get(dbList[index], readoptions, key, keySize, valueSize, &err);
    if (err) {
        fprintf(stderr, "rocksdb_get error: %s\n", err);
        assert(0);
    }
    pthread_mutex_unlock(&dbLock[index]);
}
