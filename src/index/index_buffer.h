#ifndef INDEX_BUFFER_H_
#define INDEX_BUFFER_H_

#include "../destor.h"
/*
 * The basic unit in index buffer.
 */
struct indexElem {
    containerid id;
    fingerprint fp;
};

/* The buffer size > 2 * destor.rewrite_buffer_size */
/* All fingerprints that have been looked up in the index
 * but not been updated. */
struct index_buffer {
    /* map a fingerprint to a queue of indexElem */
    /* Index all fingerprints in the index buffer. */
    GHashTable *buffered_fingerprints;
    /* The number of buffered chunks */
    int chunk_num;
};

struct index_overhead {
    uint32_t index_lookup_requests;
    /* index_lookup_base */
    uint32_t storage_buffer_hits;
    uint32_t index_buffer_hits;
    /* upgrade */
    uint32_t cache_lookup_requests;
    uint32_t cache_hits;
    uint32_t kvstore_lookup_requests;
    uint32_t kvstore_hits;
    uint32_t lookup_requests_for_unique;
    /* others */
    uint32_t kvstore_update_requests;
    uint32_t read_prefetching_units;
};

#endif
