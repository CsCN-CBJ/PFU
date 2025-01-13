#include "destor.h"
#include "jcr.h"
#include "storage/containerstore.h"
#include "storage/db.h"
#include "recipe/recipestore.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "index/index.h"
#include "index/upgrade_cache.h"
#include "index/fingerprint_cache.h"
#include "utils/cache.h"
#include "similarity.h"

static pthread_t filter_t;
static int64_t chunk_num;
extern GHashTable *upgrade_processing;
extern GHashTable *upgrade_container;
extern GHashTable *upgrade_storage_buffer;
extern containerid upgrade_storage_buffer_id;

struct{
	/* accessed in dedup phase */
	struct container *container_buffer;
	/* In order to facilitate sampling in container,
	 * we keep a list for chunks in container buffer. */
    union {
        GSequence *chunks;
        DynamicArray *chunkArray;
    };
} storage_buffer;

extern struct {
	/* g_mutex_init() is unnecessary if in static storage. */
	GMutex mutex;
	GCond cond; // index buffer is not full
	int wait_threshold;
} index_lock;

extern upgrade_lock_t upgrade_index_lock;

static void flush_container() {
    if(destor.index_category[1] != INDEX_CATEGORY_PHYSICAL_LOCALITY) return;

    // GHashTable *features = sampling(storage_buffer.chunks,
    //         g_sequence_get_length(storage_buffer.chunks));
    // index_update(features, get_container_id(storage_buffer.container_buffer));
    index_update_kvstore(storage_buffer.chunkArray, get_container_id(storage_buffer.container_buffer));

    // g_hash_table_destroy(features);
    // g_sequence_free(storage_buffer.chunks);
    // storage_buffer.chunks = g_sequence_new(free_chunk);
    dynamic_array_free_special(storage_buffer.chunkArray, free_chunk);
    storage_buffer.chunkArray = dynamic_array_new(1024);

    write_container_async(storage_buffer.container_buffer);
    storage_buffer.container_buffer = create_container();
}

static void append_chunk_to_buffer(struct chunk* c) {
    /*
    * If the chunk is unique, or be fragmented and not denied,
    * we write it to a container.
    * Fragmented indicates: sparse, or out of order and not in cache,
    */
    if (storage_buffer.container_buffer == NULL){
        storage_buffer.container_buffer = create_container();
        if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
            // storage_buffer.chunks = g_sequence_new(free_chunk);
            storage_buffer.chunkArray = dynamic_array_new(1024);
    }

    if (container_overflow(storage_buffer.container_buffer, c->size)) {
        flush_container();
    }

    if(add_chunk_to_container(storage_buffer.container_buffer, c)){
        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            jcr.unique_chunk_num++;
            jcr.unique_data_size += c->size;
        }

        if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
            struct chunk* ck = new_chunk(0);
            assert(ck);
            memcpy(&ck->fp, &c->fp, sizeof(fingerprint));
            memcpy(&ck->old_fp, &c->old_fp, sizeof(fingerprint));
            // g_sequence_append(storage_buffer.chunks, ck);
            dynamic_array_add(storage_buffer.chunkArray, ck);
        }

        VERBOSE("Filter phase: Write %dth chunk to container %lld",
                chunk_num, c->id);
    }else{
        VERBOSE("Filter phase: container %lld already has this chunk", c->id);
        c->id = get_container_id(storage_buffer.container_buffer);
        SET_CHUNK(c, CHUNK_DUPLICATE);
    }
    chunk_num++; // BUGS: 可能会多次++
}

/*
 * When a container buffer is full, we push it into container_queue.
 */
static void* filter_thread(void *arg) {
    int enable_rewrite = 1;
    struct fileRecipeMeta* r = NULL;
    struct backupVersion* bv = NULL;
    if (job == DESTOR_UPDATE) {
        bv = jcr.new_bv;
    } else {
        bv = jcr.bv;
    }

    while (1) {
        struct chunk* c = sync_queue_pop(rewrite_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        /* reconstruct a segment */
        struct segment* s = new_segment();

        /* segment head */
        assert(CHECK_CHUNK(c, CHUNK_SEGMENT_START));
        free_chunk(c);

        c = sync_queue_pop(rewrite_queue);
        while (!(CHECK_CHUNK(c, CHUNK_SEGMENT_END))) {
            g_sequence_append(s->chunks, c);
            if (!CHECK_CHUNK(c, CHUNK_FILE_START)
                    && !CHECK_CHUNK(c, CHUNK_FILE_END))
                s->chunk_num++;

            c = sync_queue_pop(rewrite_queue);
        }
        free_chunk(c);

        /* For self-references in a segment.
         * If we find an early copy of the chunk in this segment has been rewritten,
         * the rewrite request for it will be denied to avoid repeat rewriting. */
        GHashTable *recently_rewritten_chunks = g_hash_table_new_full(g_int64_hash,
        		g_fingerprint_equal, NULL, free_chunk);
        GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash,
        			g_fingerprint_equal, NULL, free_chunk);

        pthread_mutex_lock(&index_lock.mutex);

        TIMER_DECLARE(1);
        TIMER_BEGIN(1);
        /* This function will check the fragmented chunks
         * that would be rewritten later.
         * If we find an early copy of the chunk in earlier segments,
         * has been rewritten,
         * the rewrite request for it will be denied. */
        index_check_buffer(s);

    	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
        for (; iter != end; iter = g_sequence_iter_next(iter)) {
            c = g_sequence_get(iter);

    		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
    			continue;

            VERBOSE("Filter phase: %dth chunk in %s container %lld", chunk_num,
                    CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER) ? "out-of-order" : "", c->id);

            /* Cache-Aware Filter */
            if (destor.rewrite_enable_cache_aware && restore_aware_contains(c->id)) {
                assert(c->id != TEMPORARY_ID);
                VERBOSE("Filter phase: %dth chunk is cached", chunk_num);
                SET_CHUNK(c, CHUNK_IN_CACHE);
            }

            /* A cfl-switch for rewriting out-of-order chunks. */
            if (destor.rewrite_enable_cfl_switch) {
                double cfl = restore_aware_get_cfl();
                if (enable_rewrite && cfl > destor.rewrite_cfl_require) {
                    VERBOSE("Filter phase: Turn OFF the (out-of-order) rewrite switch of %.3f",
                            cfl);
                    enable_rewrite = 0;
                } else if (!enable_rewrite && cfl < destor.rewrite_cfl_require) {
                    VERBOSE("Filter phase: Turn ON the (out-of-order) rewrite switch of %.3f",
                            cfl);
                    enable_rewrite = 1;
                }
            }

            if(CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id == TEMPORARY_ID){
            	struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &c->fp);
            	assert(ruc);
            	c->id = ruc->id;
            }
            struct chunk* rwc = g_hash_table_lookup(recently_rewritten_chunks, &c->fp);
            if(rwc){
            	c->id = rwc->id;
            	SET_CHUNK(c, CHUNK_REWRITE_DENIED);
            }

            /* A fragmented chunk will be denied if it has been rewritten recently */
            if (!CHECK_CHUNK(c, CHUNK_DUPLICATE) 
					|| (!CHECK_CHUNK(c, CHUNK_REWRITE_DENIED)
            		&& (CHECK_CHUNK(c, CHUNK_SPARSE)
                    || (enable_rewrite && CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
                        && !CHECK_CHUNK(c, CHUNK_IN_CACHE))))) {
                /*
                 * If the chunk is unique, or be fragmented and not denied,
                 * we write it to a container.
                 * Fragmented indicates: sparse, or out of order and not in cache,
                 */
                if (storage_buffer.container_buffer == NULL){
                	storage_buffer.container_buffer = create_container();
                	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY)
                		storage_buffer.chunks = g_sequence_new(free_chunk);
                }

                if (container_overflow(storage_buffer.container_buffer, c->size)) {

                    if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
                        /*
                         * TO-DO
                         * Update_index for physical locality
                         */
                        GHashTable *features = sampling(storage_buffer.chunks,
                        		g_sequence_get_length(storage_buffer.chunks));
                        index_update(features, get_container_id(storage_buffer.container_buffer));

                        // do_update index
                        if (job == DESTOR_UPDATE && destor.upgrade_level != UPGRADE_NAIVE) {
                            pthread_mutex_lock(&upgrade_index_lock.mutex);
                            assert(0);
                            // upgrade_index_update(storage_buffer.chunks, get_container_id(storage_buffer.container_buffer));
                            pthread_mutex_unlock(&upgrade_index_lock.mutex);
                        }

                        g_hash_table_destroy(features);
                        g_sequence_free(storage_buffer.chunks);
                        storage_buffer.chunks = g_sequence_new(free_chunk);
                    }
                    TIMER_END(1, jcr.filter_time);
                    write_container_async(storage_buffer.container_buffer);
                    TIMER_BEGIN(1);
                    storage_buffer.container_buffer = create_container();
                }

                if(add_chunk_to_container(storage_buffer.container_buffer, c)){

                	struct chunk* wc = new_chunk(0);
                	memcpy(&wc->fp, &c->fp, sizeof(fingerprint));
                	wc->id = c->id;
                	if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
                		jcr.unique_chunk_num++;
                		jcr.unique_data_size += c->size;
                		g_hash_table_insert(recently_unique_chunks, &wc->fp, wc);
                    	VERBOSE("Filter phase: %dth chunk is recently unique, size %d", chunk_num,
                    			g_hash_table_size(recently_unique_chunks));
                	} else {
                		jcr.rewritten_chunk_num++;
                		jcr.rewritten_chunk_size += c->size;
                		g_hash_table_insert(recently_rewritten_chunks, &wc->fp, wc);
                	}

                	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
                		struct chunk* ck = new_chunk(0);
                		memcpy(&ck->fp, &c->fp, sizeof(fingerprint));
                        memcpy(&ck->old_fp, &c->old_fp, sizeof(fingerprint));
                		g_sequence_append(storage_buffer.chunks, ck);
                	}

                	VERBOSE("Filter phase: Write %dth chunk to container %lld",
                			chunk_num, c->id);
                }else{
                	VERBOSE("Filter phase: container %lld already has this chunk", c->id);
            		assert(destor.index_category[0] != INDEX_CATEGORY_EXACT
            				|| destor.rewrite_algorithm[0]!=REWRITE_NO);
                }

            }else{
                if(CHECK_CHUNK(c, CHUNK_REWRITE_DENIED)){
                    VERBOSE("Filter phase: %lldth fragmented chunk is denied", chunk_num);
                }else if (CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)) {
                    VERBOSE("Filter phase: %lldth chunk in out-of-order container %lld is already cached",
                            chunk_num, c->id);
                }
            }

            assert(c->id != TEMPORARY_ID);

            /* Collect historical information. */
            // har_monitor_update(c->id, c->size);

            /* Restore-aware */
            // restore_aware_update(c->id, c->size);

            chunk_num++;
        }

        int full = index_update_buffer(s);

        /* Write a SEGMENT_BEGIN */
        segmentid sid = append_segment_flag(bv, CHUNK_SEGMENT_START, s->chunk_num);

        /* Write recipe */
    	iter = g_sequence_get_begin_iter(s->chunks);
    	end = g_sequence_get_end_iter(s->chunks);
        for (; iter != end; iter = g_sequence_iter_next(iter)) {
            c = g_sequence_get(iter);

        	if(r == NULL){
        		assert(CHECK_CHUNK(c,CHUNK_FILE_START));
        		r = new_file_recipe_meta(c->data);
        	}else if(!CHECK_CHUNK(c,CHUNK_FILE_END)){
        		struct chunkPointer cp;
        		cp.id = c->id;
        		assert(cp.id>=0);
        		memcpy(&cp.fp, &c->fp, sizeof(fingerprint));
        		cp.size = c->size;
        		append_n_chunk_pointers(bv, &cp ,1);
        		r->chunknum++;
        		r->filesize += c->size;

    	    	jcr.chunk_num++;
	    	    jcr.data_size += c->size;

        	}else{
        		assert(CHECK_CHUNK(c,CHUNK_FILE_END));
        		append_file_recipe_meta(bv, r);
        		free_file_recipe_meta(r);
        		r = NULL;

	            jcr.file_num++;
        	}
        }

       	/* Write a SEGMENT_END */
       	append_segment_flag(bv, CHUNK_SEGMENT_END, 0);

        if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY){
             /*
              * TO-DO
              * Update_index for logical locality
              */
            s->features = sampling(s->chunks, s->chunk_num);
         	if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
         		/*
         		 * For exact deduplication,
         		 * unique fingerprints are inserted.
         		 */
         		VERBOSE("Filter phase: add %d unique fingerprints to %d features",
         				g_hash_table_size(recently_unique_chunks),
         				g_hash_table_size(s->features));
         		GHashTableIter iter;
         		gpointer key, value;
         		g_hash_table_iter_init(&iter, recently_unique_chunks);
         		while(g_hash_table_iter_next(&iter, &key, &value)){
         			struct chunk* uc = value;
         			fingerprint *ft = malloc(sizeof(fingerprint));
         			memcpy(ft, &uc->fp, sizeof(fingerprint));
         			g_hash_table_insert(s->features, ft, NULL);
         		}

         		/*
         		 * OPTION:
         		 * 	It is still an open problem whether we need to update
         		 * 	rewritten fingerprints.
         		 * 	It would increase index update overhead, while the benefit
         		 * 	remains unclear.
         		 * 	More experiments are required.
         		 */
         		VERBOSE("Filter phase: add %d rewritten fingerprints to %d features",
         				g_hash_table_size(recently_rewritten_chunks),
         				g_hash_table_size(s->features));
         		g_hash_table_iter_init(&iter, recently_rewritten_chunks);
         		while(g_hash_table_iter_next(&iter, &key, &value)){
         			struct chunk* uc = value;
         			fingerprint *ft = malloc(sizeof(fingerprint));
         			memcpy(ft, &uc->fp, sizeof(fingerprint));
         			g_hash_table_insert(s->features, ft, NULL);
         		}
         	}
         	index_update(s->features, sid);
         }

        free_segment(s);

        if(index_lock.wait_threshold > 0 && full == 0){
        	pthread_cond_broadcast(&index_lock.cond);
        }
        TIMER_END(1, jcr.filter_time);
        pthread_mutex_unlock(&index_lock.mutex);

        g_hash_table_destroy(recently_rewritten_chunks);
        g_hash_table_destroy(recently_unique_chunks);

    }

    if (storage_buffer.container_buffer
    		&& !container_empty(storage_buffer.container_buffer)){
        if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
            /*
             * TO-DO
             * Update_index for physical locality
             */
        	GHashTable *features = sampling(storage_buffer.chunks,
        			g_sequence_get_length(storage_buffer.chunks));
        	index_update(features, get_container_id(storage_buffer.container_buffer));

            // do_update index
            if (job == DESTOR_UPDATE && destor.upgrade_level != UPGRADE_NAIVE) {
                pthread_mutex_lock(&upgrade_index_lock.mutex);
                upgrade_index_update(storage_buffer.chunks, get_container_id(storage_buffer.container_buffer));
                pthread_mutex_unlock(&upgrade_index_lock.mutex);
            }

        	g_hash_table_destroy(features);
        	g_sequence_free(storage_buffer.chunks);
        }
        write_container_async(storage_buffer.container_buffer);
    }

    /* All files done */
    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

static void append_chunk_sequence(struct backupVersion* bv, struct fileRecipeMeta* r, GSequence *file_chunks) {
    // iter seq
    struct chunk *c;
    GSequenceIter *iter = g_sequence_get_begin_iter(file_chunks);
    GSequenceIter *end = g_sequence_get_end_iter(file_chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        c = g_sequence_get(iter);
        assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
        struct chunkPointer cp;
        cp.id = c->id;
        assert(cp.id>=0);
        memcpy(&cp.fp, &c->fp, sizeof(fingerprint));
        cp.size = c->size;
        append_n_chunk_pointers(bv, &cp ,1);
        r->chunknum++;
        r->filesize += c->size;

        jcr.chunk_num++;
        jcr.data_size += c->size;
    }
}

static void append_chunk_array(struct backupVersion* bv, struct fileRecipeMeta* r, DynamicArray *file_chunks) {
    // iter seq
    struct chunk *c;
    for (int i = 0; i < file_chunks->size; i++) {
        c = file_chunks->data[i];
        assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
        struct chunkPointer cp;
        cp.id = c->id;
        assert(cp.id>=0);
        memcpy(&cp.fp, &c->fp, sizeof(fingerprint));
        cp.size = c->size;
        append_n_chunk_pointers(bv, &cp ,1);
        r->chunknum++;
        r->filesize += c->size;

        jcr.chunk_num++;
        jcr.data_size += c->size;
    }
}

static void append_chunk_cks(struct backupVersion* bv, struct fileRecipeMeta* r, struct chunk *file_chunks, int size) {
    // iter seq
    struct chunk *c;
    for (int i = 0; i < size; i++) {
        c = file_chunks + i;
        assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
        struct chunkPointer cp;
        cp.id = c->id;
        assert(cp.id>=0);
        memcpy(&cp.fp, &c->fp, sizeof(fingerprint));
        cp.size = c->size;
        append_n_chunk_pointers(bv, &cp ,1);
        r->chunknum++;
        r->filesize += c->size;

        jcr.chunk_num++;
        jcr.data_size += c->size;
    }
}


typedef struct {
    struct fileRecipeMeta *recipe;
    DynamicArray **file_chunks_list;
    int count;
    int total;
} recipeCache_t;

recipeCache_t *init_recipe_cache(int total) {
    recipeCache_t *rc = malloc(sizeof(recipeCache_t));
    rc->file_chunks_list = calloc(total, sizeof(GSequence *));
    rc->count = 0;
    rc->total = total;
    return rc;
}

void* filter_thread_container(void* arg) {
	pthread_setname_np(pthread_self(), "filter");
    GHashTable *htb = NULL;
    struct container *con;
    struct chunk *ck;
    containerid id;
    while ((con = sync_queue_pop(hash_queue)) != NULL) {
        TIMER_DECLARE(1);
        TIMER_BEGIN(1);

        // container start
        assert(htb == NULL);
        htb = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
        id = con->meta.id;
        assert(id >= 0);

        // in container
        for (int i = 0; i < con->meta.chunk_num; i++) {
            ck = con->chunks + i;
            append_chunk_to_buffer(ck);
            assert(ck->id >= 0);

            upgrade_index_kv_t *kvp;
            kvp = (upgrade_index_kv_t *)malloc(sizeof(upgrade_index_kv_t));
            memcpy(kvp->old_fp, ck->old_fp, sizeof(fingerprint));
            memcpy(kvp->value.fp, ck->fp, sizeof(fingerprint));
            kvp->value.id = ck->id;
            g_hash_table_insert(htb, &kvp->old_fp, &kvp->value);
        }

        // container end
        pthread_mutex_lock(&upgrade_index_lock.mutex);
        // insert into external cache
        DEBUG("Process container %ld, %ld chunks", id, g_hash_table_size(htb));
        upgrade_external_cache_insert(id, htb);
        pthread_mutex_unlock(&upgrade_index_lock.mutex);

        htb = NULL;
        free_container(con);
        TIMER_END(1, jcr.filter_time);
    }
    sync_queue_term(hash_queue);
    if (storage_buffer.container_buffer
    		&& !container_empty(storage_buffer.container_buffer)){
        flush_container();
    }
    /* All files done */
    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

void* filter_thread_recipe(void* arg) {
	pthread_setname_np(pthread_self(), "recipe_filter");
	struct backupVersion* bv = jcr.new_bv;
    DynamicArray *file_chunks = NULL;
    recipeUnit_t *ru = NULL;
    while ((ru = sync_queue_pop(hash_queue)) != NULL) {
        TIMER_DECLARE(1);
        TIMER_BEGIN(1);
        write_n_chunks(bv, ru->cks, ru->chunk_num, ru->chunk_off);
        if (ru->sub_id == ru->total_num - 1) {
            jcr.file_num++;
        }
        free_file_recipe_meta(ru->recipe);
        free(ru->cks);
        free(ru);
        TIMER_END(1, jcr.filter_time);
    }
    jcr.status = JCR_STATUS_DONE;
    return NULL;
}

void start_filter_phase() {

	storage_buffer.container_buffer = NULL;

    // init_restore_aware();

    if (job == DESTOR_UPDATE) {
        assert(destor.upgrade_level == UPGRADE_NAIVE);
        pthread_create(&filter_t, NULL, filter_thread, NULL);
    } else {
        pthread_create(&filter_t, NULL, filter_thread, NULL);
    }
}

void stop_filter_phase() {
    pthread_join(filter_t, NULL);
    if (destor.upgrade_level == UPGRADE_NAIVE || destor.upgrade_level == UPGRADE_1D_RELATION)
        close_har();
	NOTICE("filter phase stops successfully!");

}
