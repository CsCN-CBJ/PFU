#include "index.h"
#include "kvstore.h"
#include "fingerprint_cache.h"
#include "index_buffer.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "../jcr.h"
#include "../storage/db.h"

struct index_overhead index_overhead;
struct index_overhead upgrade_index_overhead;
GHashTable *upgrade_processing;

struct index_buffer index_buffer;

gboolean g_feature_equal(char* a, char* b){
	return !memcmp(a, b, destor.index_key_size);
}

guint g_feature_hash(char *feature){
	int i, hash = 0;
	for(i=0; i<destor.index_key_size; i++){
		hash += feature[i] << (8*i);
	}
	return hash;
}

extern void init_segmenting_method();
extern void init_sampling_method();

void init_index() {
    /* Do NOT assign a free function for value. */
    index_buffer.buffered_fingerprints = g_hash_table_new_full(g_int64_hash,
            g_fingerprint_equal, NULL, NULL);
    index_buffer.chunk_num = 0;

    if(destor.index_specific != INDEX_SPECIFIC_NO){
        destor.index_key_size = sizeof(fingerprint);
        switch(destor.index_specific){
            case INDEX_SPECIFIC_DDFS:{
                destor.index_category[0] = INDEX_CATEGORY_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
                break;
            }
            case INDEX_SPECIFIC_BLOCK_LOCALITY_CACHING:{
                destor.index_category[0] = INDEX_CATEGORY_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;
                destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
                destor.index_sampling_method[1] = 1;

                destor.index_segment_prefech = destor.index_segment_prefech > 1 ?
                destor.index_segment_prefech : 16;
                break;
            }
            case INDEX_SPECIFIC_SAMPLED:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;

                destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
                destor.index_sampling_method[1] = destor.index_sampling_method[1] > 1 ?
                destor.index_sampling_method[1] : 128;
                break;
            }
            case INDEX_SPECIFIC_SPARSE:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;

                destor.index_segment_algorithm[0] = INDEX_SEGMENT_CONTENT_DEFINED;

                destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;

                destor.index_sampling_method[0] = INDEX_SAMPLING_RANDOM;
                destor.index_sampling_method[1] = destor.index_sampling_method[1] > 1 ?
                destor.index_sampling_method[1] : 128;

                destor.index_segment_prefech = 1;
                break;
            }
            case INDEX_SPECIFIC_SILO:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;

                destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;

                destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
                destor.index_segment_selection_method[1] = 1;

                destor.index_sampling_method[0] = INDEX_SAMPLING_MIN;
                destor.index_sampling_method[1] = 0;

                destor.index_segment_prefech = destor.index_segment_prefech > 1 ?
                destor.index_segment_prefech : 16;
                break;
            }
            default:{
                WARNING("Invalid index specific!");
                exit(1);
            }
        }
    }

    if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
        destor.index_key_size = sizeof(fingerprint);
    }

    if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
        destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
        if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
            destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
            destor.index_sampling_method[1] = 1;
        }
    }

    assert(destor.index_key_size > 0 && destor.index_key_size <= sizeof(fingerprint));

    init_sampling_method();
    init_segmenting_method();

    init_kvstore();

    init_fingerprint_cache();
    init_upgrade_fingerprint_cache();
    upgrade_processing = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);

    memset(&index_overhead, 0, sizeof(struct index_overhead));
    memset(&upgrade_index_overhead, 0, sizeof(struct index_overhead));

    NOTICE("Init index module successfully");
}

void close_index() {
    close_kvstore();
    // close_upgrade_kvstore();
    assert(g_hash_table_size(upgrade_processing) == 0);
    g_hash_table_destroy(upgrade_processing);
}

extern struct{
    /* accessed in dedup phase */
    struct container *container_buffer;
    /* In order to facilitate sampling in container,
     * we keep a queue for chunks in container buffer. */
    GSequence *chunks;
} storage_buffer;

static void index_lookup_base(struct segment *s){

    GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        index_overhead.index_lookup_requests++;
        /* First check it in the storage buffer */
        if(storage_buffer.container_buffer
                && !CHECK_CHUNK(c, CHUNK_DUPLICATE)
                && lookup_fingerprint_in_container(storage_buffer.container_buffer, &c->fp)){
            index_overhead.storage_buffer_hits++;
            c->id = get_container_id(storage_buffer.container_buffer);
            SET_CHUNK(c, CHUNK_DUPLICATE);
            SET_CHUNK(c, CHUNK_REWRITE_DENIED);
        }

        /*
         * First check the buffered fingerprints,
         * recently backup fingerprints.
         */
        GQueue *tq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
        if (!tq) {
            tq = g_queue_new();
        } else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            index_overhead.index_buffer_hits++;
            struct indexElem *be = g_queue_peek_head(tq);
            c->id = be->id;
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }

        /* Check the fingerprint cache */
        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            /* Searching in fingerprint cache */
            index_overhead.cache_lookup_requests++;
            int64_t id = fingerprint_cache_lookup(&c->fp);
            if(id != TEMPORARY_ID){
                index_overhead.cache_hits++;
                c->id = id;
                SET_CHUNK(c, CHUNK_DUPLICATE);
            }
        }

        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            /* Searching in key-value store */
            index_overhead.kvstore_lookup_requests++;
            int64_t* ids = kvstore_lookup((char*)&c->fp);
            if(ids){
                index_overhead.kvstore_hits++;
                /* prefetch the target unit */
                fingerprint_cache_prefetch(ids[0]);
                int64_t id = fingerprint_cache_lookup(&c->fp);
                if(id != TEMPORARY_ID){
                    /*
                     * It can be not cached,
                     * since a partial key is possible in near-exact deduplication.
                     */
                    c->id = id;
                    SET_CHUNK(c, CHUNK_DUPLICATE);
                }else{
                    NOTICE("Filter phase: A key collision occurs");
                }
                // free(ids);
            }else{
                index_overhead.lookup_requests_for_unique++;
                VERBOSE("Dedup phase: non-existing fingerprint");
            }
        }

        /* Insert it into the index buffer */
        struct indexElem *ne = (struct indexElem*) malloc(sizeof(struct indexElem));
        ne->id = c->id;
        memcpy(&ne->fp, &c->fp, sizeof(fingerprint));

        g_queue_push_tail(tq, ne);
        g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, tq);

        index_buffer.chunk_num++;
    }

}

extern void index_lookup_similarity_detection(struct segment *s);

extern struct {
    /* g_mutex_init() is unnecessary if in static storage. */
    GMutex mutex;
    GCond cond; // index buffer is not full
    // index buffer is full, waiting
    // if threshold < 0, it indicates no threshold.
    int wait_threshold;
} index_lock;

/*
 * return 1: indicates lookup is successful.
 * return 0: indicates the index buffer is full.
 */
int index_lookup(struct segment* s) {

    /* Ensure the next phase not be blocked. */
    if (index_lock.wait_threshold > 0
            && index_buffer.chunk_num >= index_lock.wait_threshold) {
        DEBUG("The index buffer is full (%d chunks in buffer)",
                index_buffer.chunk_num);
        return 0;
    }

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);
    if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY
            && destor.index_segment_selection_method[0] != INDEX_SEGMENT_SELECT_BASE){
        /* Similarity-based */
        s->features = sampling(s->chunks, s->chunk_num);
        index_lookup_similarity_detection(s);
    }else{
        /* Base */
        index_lookup_base(s);
    }
    TIMER_END(1, jcr.dedup_time);

    return 1;
}

/*
 * Input features with a container/segment ID.
 * For physical locality, this function is called for each written container.
 * For logical locality, this function is called for each written segment.
 */
void index_update(GHashTable *features, int64_t id){
    VERBOSE("Filter phase: update %d features", g_hash_table_size(features));
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, features);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        index_overhead.kvstore_update_requests++;
        kvstore_update(key, id);
    }
}

inline void index_delete(fingerprint *fp, int64_t id){
	kvstore_delete(fp, id);
}

/* This function is designed for rewriting. */
void index_check_buffer(struct segment *s) {

	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        if((CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id == TEMPORARY_ID)
                || CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
                || CHECK_CHUNK(c, CHUNK_SPARSE)){
            /*
             * 1. A duplicate chunk with a TEMPORARY_ID,
             * 	  indicates it is a copy of a recent unique chunk.
             * 2. If we want to rewrite a chunk,
             *    we check whether it has been rewritten recently.
             */
            GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
            assert(bq);
            struct indexElem* be = g_queue_peek_head(bq);

            if(be->id > c->id){

                if(c->id != TEMPORARY_ID)
                    /* this chunk has been rewritten recently */
                    SET_CHUNK(c, CHUNK_REWRITE_DENIED);

                c->id = be->id;
            }
        }
    }
}

/*
 * Return 1 indicates buffer remains full.
 */
int index_update_buffer(struct segment *s){

	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        assert(c->id != TEMPORARY_ID);

        GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);

        struct indexElem* e = g_queue_pop_head(bq);
        if(g_queue_get_length(bq) == 0){
            g_hash_table_remove(index_buffer.buffered_fingerprints, &c->fp);
            g_queue_free(bq);
        }else{
            int num = g_queue_get_length(bq), j;
            for(j=0; j<num; j++){
                struct indexElem* ie = g_queue_peek_nth(bq, j);
                ie->id = c->id;
            }
        }
        free(e);
        index_buffer.chunk_num--;
    }

    if (index_lock.wait_threshold <= 0
            || index_buffer.chunk_num < index_lock.wait_threshold) {
        DEBUG("The index buffer is ready for more chunks (%d chunks in buffer)",
                index_buffer.chunk_num);
        return 0;
    }
    return 1;
}

/**
 * index functions for do_upgrade
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
        setDB(&c->old_fp, sizeof(fingerprint), &v, sizeof(upgrade_index_value_t));
    }
}

/*
static void upgrade_index_update1_multi(GSequence *chunks, int64_t id) {
    int length = g_sequence_get_length(chunks);
    VERBOSE("Filter phase: update1 upgrade index %d features", length);
    fingerprint *fps = (fingerprint*)malloc(sizeof(fingerprint) * length);
    upgrade_index_value_t *v = (upgrade_index_value_t*)malloc(sizeof(upgrade_index_value_t) * length);
    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    int i = 0;
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);        
        upgrade_index_overhead.kvstore_update_requests++;

        memcpy(&fps[i], &c->old_fp, sizeof(fingerprint));
        v[i].id = id;
        memcpy(&v[i].fp, &c->fp, sizeof(fingerprint));
        ++i;
        // upgrade_kvstore_update((char*)&c->old_fp, &v);
    }
    assert(i == length);
    insert_sql_multi((char*)fps, sizeof(fingerprint), (char*)v, sizeof(upgrade_index_value_t), length);
    free(fps);
    free(v);
}
*/

/*
static void upgrade_index_update2(GSequence *chunks, int64_t id) {
    VERBOSE("Filter phase: update2 upgrade index %d features", g_sequence_get_length(chunks));
    upgrade_index_kv_t *kv;
    GHashTable* con = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk* c = g_sequence_get(iter);        
        upgrade_index_overhead.kvstore_update_requests++;
        
        upgrade_kvstore_update((char*)&c->old_fp, &id);

        kv = (upgrade_index_kv_t*)malloc(sizeof(upgrade_index_kv_t));
        kv->value.id = id;
        memcpy(&kv->value.fp, &c->fp, sizeof(fingerprint));
        memcpy(&kv->old_fp, &c->old_fp, sizeof(fingerprint));
        g_hash_table_insert(con, &kv->old_fp, &kv->value);
    }
    // 暂时使用containerid, 可能需要改成单独的id
    write_upgrade_index_container(con, id);
}
*/

void upgrade_index_update(GSequence *chunks, int64_t id) {
    assert(destor.upgrade_level == UPGRADE_1D_RELATION);
    if (destor.upgrade_level == UPGRADE_1D_RELATION) {
        upgrade_index_update1(chunks, id);
    } else if (destor.upgrade_level == UPGRADE_2D_RELATION) {
        assert(0);
        // upgrade_index_update2(chunks, id);
    }
}

void _upgrade_index_lookup(struct chunk *c){
    
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
        int ret = getDB(&c->old_fp, sizeof(fingerprint), &v, &valueSize);
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
        VERBOSE("_upgrade_index_lookup: non-existing fingerprint");
    }
}

void _upgrade_index_lookup_c2c(struct chunk *c) {
    // c2c: container to container
    
    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
        return;

    upgrade_index_overhead.index_lookup_requests++;

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        /* Searching in fingerprint cache */
        upgrade_index_value_t* v = upgrade_fingerprint_cache_lookup(&c->old_fp);
        upgrade_index_overhead.cache_lookup_requests++;
        if(v){
            upgrade_index_overhead.cache_hits++;
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        upgrade_index_overhead.kvstore_lookup_requests++;
        if (upgrade_fingerprint_cache_prefetch(c->id)) {
            upgrade_index_overhead.kvstore_hits++;
            upgrade_index_value_t* v = upgrade_fingerprint_cache_lookup(&c->old_fp);
            assert(v);
            assert(v->id >= 0);
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        upgrade_index_overhead.lookup_requests_for_unique++;
        VERBOSE("_upgrade_index_lookup_c2c: non-existing fingerprint");
    }
}

void upgrade_index_lookup_2D_filter(struct chunk *c) {
    // 2D index lookup in filter phase, to count separately
    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
        return;

    index_overhead.index_lookup_requests++;

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        /* Searching in fingerprint cache */
        upgrade_index_value_t* v = upgrade_fingerprint_cache_lookup(&c->old_fp);
        index_overhead.cache_lookup_requests++;
        if(v){
            index_overhead.cache_hits++;
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        index_overhead.kvstore_lookup_requests++;
        if (upgrade_fingerprint_cache_prefetch(c->id)) {
            index_overhead.kvstore_hits++;
            upgrade_index_value_t* v = upgrade_fingerprint_cache_lookup(&c->old_fp);
            assert(v);
            assert(v->id >= 0);
            c->id = v->id;
            memcpy(&c->fp, &v->fp, sizeof(fingerprint));
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }
    }

    if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
        index_overhead.lookup_requests_for_unique++;
        VERBOSE("_upgrade_index_lookup_c2c: non-existing fingerprint");
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
        _upgrade_index_lookup(c);
    } else if (destor.upgrade_level == UPGRADE_2D_RELATION) {
        _upgrade_index_lookup_c2c(c);
    }

    TIMER_END(1, jcr.pre_dedup_time);

    return 1;
}

void print_index_overhead(FILE *fp, struct index_overhead *overhead) {
    for(int i = 0; i < sizeof(struct index_overhead) / sizeof(uint32_t); i++) {
        fprintf(fp, "%u ", ((uint32_t *)overhead)[i]);
    }
}
