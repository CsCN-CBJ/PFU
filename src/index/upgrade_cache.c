#include "upgrade_cache.h"
#include "index.h"
#include "../utils/lru_cache.h"
#include "../storage/containerstore.h"
#include "../storage/db.h"
#include "../jcr.h"

extern struct index_overhead index_overhead;
struct index_overhead upgrade_index_overhead;
GHashTable *upgrade_processing;
GHashTable *upgrade_container;
GHashTable *upgrade_storage_buffer = NULL; // 确保当前在storage_buffer中的container不会被LRU踢出

void init_upgrade_index() {
    upgrade_processing = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
    upgrade_container = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
    memset(&upgrade_index_overhead, 0, sizeof(struct index_overhead));
}

void close_upgrade_index() {
    assert(g_hash_table_size(upgrade_processing) == 0);
    g_hash_table_destroy(upgrade_processing);
    g_hash_table_destroy(upgrade_container);
}

/**
 * 1D
*/
static void upgrade_index_update1(GSequence *chunks, int64_t id) {
    int length = g_sequence_get_length(chunks);
    VERBOSE("Filter phase: update1 upgrade index %d features", length);
    upgrade_index_value_t v;
    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);        
        upgrade_index_overhead.kvstore_update_requests++;
        v.id = id;
        memcpy(&v.fp, &c->fp, sizeof(fingerprint));
        setDB(DB_UPGRADE, &c->old_fp, sizeof(fingerprint), &v, sizeof(upgrade_index_value_t));
    }
}

void upgrade_index_update(GSequence *chunks, int64_t id) {
    assert(destor.upgrade_level == UPGRADE_1D_RELATION);
    upgrade_index_update1(chunks, id);
}

void upgrade_index_lookup_1D(struct chunk *c){
    
    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
        return;

    upgrade_index_overhead.index_lookup_requests++;

    /* First check it in the storage buffer */

    /*
    * First check the buffered fingerprints,
    * recently backup fingerprints.
    */

    /* Check the fingerprint cache */
    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        /* Searching in fingerprint cache */
        upgrade_index_value_t* v = upgrade_1D_fingerprint_cache_lookup(&c->old_fp);
        upgrade_index_overhead.cache_lookup_requests++;
        if(v){
            upgrade_index_overhead.cache_hits++;
            VERBOSE("Pre Dedup phase: existing fingerprint");
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        /* Searching in key-value store */
        upgrade_index_value_t *v;
        size_t valueSize;
        int ret = getDB(DB_UPGRADE, &c->old_fp, sizeof(fingerprint), &v, &valueSize);
        upgrade_index_overhead.kvstore_lookup_requests++;
        if(!ret) {
            assert(valueSize == sizeof(upgrade_index_value_t));
            upgrade_index_overhead.kvstore_hits++;
            upgrade_index_overhead.read_prefetching_units++;
            VERBOSE("Pre Dedup phase: lookup kvstore for existing");
            upgrade_1D_fingerprint_cache_insert(&c->old_fp, v);
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
            free(v);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        upgrade_index_overhead.lookup_requests_for_unique++;
        VERBOSE("upgrade_index_lookup_1D: non-existing fingerprint");
    }
}


/**
 * 2D
 */
void _upgrade_dedup_buffer(struct chunk *c, struct index_overhead *stats) {
    if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) return;

    /* Searching in fingerprint cache */
    upgrade_index_value_t* v = NULL;
    if (upgrade_storage_buffer) {
        v = g_hash_table_lookup(upgrade_storage_buffer, &c->old_fp);
    }
    if (!v) {
        v = upgrade_fingerprint_cache_lookup(c);
    }
    stats->cache_lookup_requests++;
    if(v){
        stats->cache_hits++;
        if (destor.fake_containers) {
            c->id = 1;
            memcpy(&c->fp, &c->old_fp, sizeof(fingerprint));
        } else {
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
        }
        SET_CHUNK(c, CHUNK_DUPLICATE);
    }
}

void _upgrade_dedup_kvstore(struct chunk *c, struct index_overhead *stats) {
    if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) return;

    stats->kvstore_lookup_requests++;
    if (upgrade_fingerprint_cache_prefetch(c->id)) {
        stats->kvstore_hits++;
        upgrade_index_value_t* v = upgrade_fingerprint_cache_lookup(c);
        assert(v);
        assert(v->id >= 0);
        c->id = v->id;
        memcpy(&c->fp, &v->fp, sizeof(fingerprint));
        SET_CHUNK(c, CHUNK_DUPLICATE);
    }
}

void upgrade_index_lookup_2D(struct chunk *c, struct index_overhead *stats, int constrained) {
    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
        return;

    stats->index_lookup_requests++;

    _upgrade_dedup_buffer(c, stats);

    if (!constrained) {
        _upgrade_dedup_kvstore(c, stats);
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        stats->lookup_requests_for_unique++;
        VERBOSE("upgrade_index_lookup_2D: non-existing fingerprint");
    }
}

void upgrade_index_lookup_2D_filter(struct chunk *c) {
    // 2D index lookup in filter phase, to count separately
    if (destor.upgrade_level == UPGRADE_2D_RELATION) {
        upgrade_index_lookup_2D(c, &index_overhead, 0);
    } else if (destor.upgrade_level == UPGRADE_SIMILARITY || destor.upgrade_level == UPGRADE_2D_CONSTRAINED) {
        upgrade_index_lookup_2D(c, &index_overhead, 1);
    } else {
        assert(0);
    }
}

/*
 * return 1: indicates lookup is successful.
 * return 0: indicates the index buffer is full.
 */
int upgrade_index_lookup(struct chunk* c) {

    /* Ensure the next phase not be blocked. */
    // if (index_lock.wait_threshold > 0
    //         && index_buffer.chunk_num >= index_lock.wait_threshold) {
    //     DEBUG("The index buffer is full (%d chunks in buffer)",
    //             index_buffer.chunk_num);
    //     return 0;
    // }

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    if (destor.upgrade_level == UPGRADE_1D_RELATION) {
        upgrade_index_lookup_1D(c);
    } else if (destor.upgrade_level == UPGRADE_2D_RELATION) {
        upgrade_index_lookup_2D(c, &upgrade_index_overhead, 0);
    } else if (destor.upgrade_level == UPGRADE_SIMILARITY || destor.upgrade_level == UPGRADE_2D_CONSTRAINED) {
        upgrade_index_lookup_2D(c, &upgrade_index_overhead, 1);
    } else {
        assert(0);
    }

    TIMER_END(1, jcr.pre_dedup_time);

    return 1;
}


/**
 * Upgrade fingerprint cache
 * LRU of GHashTable(old_fp, upgrade_index_value_t)
*/
static struct lruCache* upgrade_lru_queue;
static lruHashMap_t *upgrade_cache;
GHashTable *upgrade_cache_htb;

void free_upgrade_index_value(GHashTable **htb) {
	g_hash_table_destroy(*htb);
	free(htb);
}

int compare_upgrade_index_value(GHashTable **htb, fingerprint *old_fp) {
	return g_hash_table_lookup(*htb, old_fp) != NULL;
}

void init_upgrade_1D_fingerprint_cache();
void init_upgrade_fingerprint_cache() {
	if (destor.upgrade_level == UPGRADE_1D_RELATION) {
		init_upgrade_1D_fingerprint_cache();
		return;
	}
	if (destor.fake_containers) {
		upgrade_cache = new_lru_hashmap(destor.index_cache_size - 1, NULL, g_int64_hash, g_int64_equal);
	} else {
		upgrade_cache = new_lru_hashmap(destor.index_cache_size - 1, g_hash_table_destroy, g_int64_hash, g_int64_equal);
	}
}

upgrade_index_value_t* upgrade_fingerprint_cache_lookup(struct chunk* c) {
	GHashTable *htb = lru_hashmap_lookup(upgrade_cache, &c->id);
	if (htb) {
		if (destor.fake_containers) return (upgrade_index_value_t*)1;
		
		upgrade_index_value_t* v = g_hash_table_lookup(htb, &c->old_fp);
		assert(v);
		return v;
	}
	return NULL;
}

void upgrade_fingerprint_cache_insert(containerid id, GHashTable *htb) {
	containerid *id_p = malloc(sizeof(containerid));
	*id_p = id;

	if (destor.fake_containers) {
		g_hash_table_destroy(htb);
		lru_hashmap_insert(upgrade_cache, id_p, "1");
	} else {
		lru_hashmap_insert(upgrade_cache, id_p, htb);
	}
}

/**
 * return 0 if not found
*/
int upgrade_fingerprint_cache_prefetch(containerid id) {
	assert(destor.upgrade_level == UPGRADE_2D_RELATION);
	int bufferSize = sizeof(upgrade_index_kv_t) * MAX_META_PER_CONTAINER;
	upgrade_index_kv_t *kv; // sql insertion buffer
	size_t valueSize;
	int ret = getDB(DB_UPGRADE, &id, sizeof(containerid), &kv, &valueSize);
	upgrade_index_overhead.read_prefetching_units++;
	if (ret) {
		DEBUG("upgrade_fingerprint_cache_prefetch: The index container %lld has not been written!", id);
		return 0;
	}
	if (valueSize % sizeof(upgrade_index_kv_t) != 0 || valueSize == 0) {
		WARNING("Error! valueSize = %d", valueSize);
		exit(1);
	}
	
	GHashTable *c = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
	for (int i = 0; i < valueSize / sizeof(upgrade_index_kv_t); i++) {
		upgrade_index_kv_t *kv_i = malloc(sizeof(upgrade_index_kv_t));
		memcpy(kv_i, kv + i, sizeof(upgrade_index_kv_t));
		g_hash_table_insert(c, &kv_i->old_fp, &kv_i->value);
	}
	free(kv);
	upgrade_fingerprint_cache_insert(id, c);
	return 1;
}

/**
 * 1D
 * LRU(upgrade_index_kv_t)
 * +
 * GHashTable(old_fp, upgrade_index_kv_t)
 * 
 * LRU free: do_nothing
 * GHashTable free: free upgrade_index_kv_t
*/

void init_upgrade_1D_fingerprint_cache() {
	upgrade_lru_queue = new_lru_cache(destor.index_cache_size * 818,
				free, NULL); // 不会查找，所以不需要比较函数
	upgrade_cache_htb = g_hash_table_new_full(g_feature_hash, g_feature_equal, NULL, NULL);
}

upgrade_index_value_t* upgrade_1D_fingerprint_cache_lookup(fingerprint *old_fp) {
	GList *elem =  g_hash_table_lookup(upgrade_cache_htb, old_fp);
	if (!elem) {
		return NULL;
	}
	upgrade_lru_queue->elem_queue = g_list_remove_link(upgrade_lru_queue->elem_queue, elem);
	upgrade_lru_queue->elem_queue = g_list_concat(elem, upgrade_lru_queue->elem_queue);
	upgrade_index_kv_t *kv = elem->data;
	return &kv->value;
}

void upgrade_1D_remove(void *victim, void *user_data) {
	upgrade_index_kv_t *kv = (upgrade_index_kv_t *)victim;
	g_hash_table_remove(upgrade_cache_htb, &kv->old_fp);
}

void upgrade_1D_fingerprint_cache_insert(fingerprint *old_fp, upgrade_index_value_t *v) {
	upgrade_index_kv_t *kv = (upgrade_index_kv_t *)malloc(sizeof(upgrade_index_kv_t));
	memcpy(&kv->old_fp, old_fp, sizeof(fingerprint));
	memcpy(&kv->value, v, sizeof(upgrade_index_value_t));
	lru_cache_insert(upgrade_lru_queue, kv, upgrade_1D_remove, NULL);
	GList *elem = g_list_first(upgrade_lru_queue->elem_queue);
	g_hash_table_insert(upgrade_cache_htb, &kv->old_fp, elem);
}

