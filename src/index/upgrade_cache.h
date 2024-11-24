/**
 * @file upgrade_cache.h
 * @brief Upgrade fingerprint cache
 * Created on: 2024-10-3
 * Author: Boju Chen
 */

#ifndef UPGRADE_CACHE_H_
#define UPGRADE_CACHE_H_

#include "../destor.h"
#include "../recipe/recipestore.h"
#include "upgrade_external.h"

typedef struct {
    int64_t id;
    fingerprint fp;
} upgrade_index_value_t;

typedef struct {
    fingerprint old_fp;
    upgrade_index_value_t value;
} upgrade_index_kv_t;

struct containerMap {
    containerid old_id;
    containerid new_id;
    int16_t container_num;
};

void init_upgrade_index();
void close_upgrade_index();

int upgrade_index_lookup(struct chunk *c);
void upgrade_index_update(GSequence *chunks, int64_t id);

void upgrade_index_lookup_2D_filter(struct chunk *c);

upgrade_index_value_t* upgrade_fingerprint_cache_lookup(struct chunk* c);
void upgrade_fingerprint_cache_insert(containerid id, GHashTable *htb);
void upgrade_fingerprint_cache_insert_buffer(containerid id, upgrade_index_kv_t *buf, int size);

upgrade_index_value_t* upgrade_1D_fingerprint_cache_lookup(fingerprint *old_fp);
void upgrade_1D_fingerprint_cache_insert(fingerprint *old_fp, upgrade_index_value_t *v);

void count_cache_hit(struct chunkPointer* cps, int64_t chunk_num);

#endif /* UPGRADE_CACHE_H_ */
