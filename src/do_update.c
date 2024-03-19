#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "update.h"
#include "backup.h"
#include "index/index.h"

/* defined in index.c */
extern struct index_overhead index_overhead, upgrade_index_overhead;

struct {
	/* g_mutex_init() is unnecessary if in static storage. */
	pthread_mutex_t mutex;
	pthread_cond_t cond; // index buffer is not full
	// index buffer is full, waiting
	// if threshold < 0, it indicates no threshold.
	int wait_threshold;
} upgrade_index_lock;

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(update_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

			struct chunk* c = new_chunk(0);
			memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
			assert(!memcmp(c->fp + 20, zero_fp, 12));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);

			sync_queue_push(update_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(update_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	sync_queue_term(update_recipe_queue);
	return NULL;
}

static void* lru_get_chunk_thread(void *arg) {
	struct lruCache *cache;
	// if (destor.simulation_level >= SIMULATION_RESTORE)
	cache = new_lru_cache(destor.restore_cache[1], free_container,
			lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(update_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(upgrade_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		// if (destor.simulation_level >= SIMULATION_RESTORE) {
		struct container *con = lru_cache_lookup(cache, &c->fp);
		if (!con) {
			con = retrieve_container_by_id(c->id);
			lru_cache_insert(cache, con, NULL, NULL);
			jcr.read_container_num++;
		}
		struct chunk *rc = get_chunk_in_container(con, &c->fp);
		memcpy(rc->pre_fp, c->fp, sizeof(fingerprint));
		rc->id = TEMPORARY_ID;
		assert(rc);
		TIMER_END(1, jcr.read_chunk_time);

		sync_queue_push(upgrade_chunk_queue, rc);

		// filter_phase已经算过一遍了
		// jcr.data_size += c->size;
		// jcr.chunk_num++;
		free_chunk(c);
	}

	sync_queue_term(upgrade_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

static void* pre_dedup_thread(void *arg) {
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_chunk_queue);

		if (c == NULL) {
			sync_queue_term(pre_dedup_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(pre_dedup_queue, c);
			continue;
		}

		if (destor.upgrade_level == 1) {
			/* Each duplicate chunk will be marked. */
			pthread_mutex_lock(&upgrade_index_lock.mutex);
			// while (upgrade_index_lookup(c) == 0) { // 目前永远是1, 所以不用管cond
			// 	pthread_cond_wait(&upgrade_index_lock.cond, &upgrade_index_lock.mutex);
			// }
			upgrade_index_lookup(c);
			pthread_mutex_unlock(&upgrade_index_lock.mutex);
			
		}

		sync_queue_push(pre_dedup_queue, c);
	}
	sync_queue_term(pre_dedup_queue);

	return NULL;
}

static void* sha256_thread(void* arg) {
	// char code[41];
	while (1) {
		struct chunk* c = sync_queue_pop(pre_dedup_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		jcr.hash_num++;
		SHA256_CTX ctx;
		SHA256_Init(&ctx);
		SHA256_Update(&ctx, c->data, c->size);
		SHA256_Final(c->fp, &ctx);
		TIMER_END(1, jcr.hash_time);

		// hash2code(c->fp, code);
		// code[40] = 0;
		// VERBOSE("Update hash phase: %ldth chunk identified by %s", chunk_num++, code);

		sync_queue_push(hash_queue, c);
	}
	return NULL;
}

void do_update(int revision, char *path) {

	init_recipe_store();
	init_container_store();
	init_index();

	init_update_jcr(revision, path);
	pthread_mutex_init(&upgrade_index_lock.mutex, NULL);

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "new job id: %d", jcr.new_id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "new backup path: %s", jcr.new_bv->path);
	destor_log(DESTOR_NOTICE, "update to: %s", jcr.path);

	update_recipe_queue = sync_queue_new(100);
	upgrade_chunk_queue = sync_queue_new(100);
	pre_dedup_queue = sync_queue_new(100);
	hash_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== update begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, pre_dedup_t, hash_t;
	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
    pthread_create(&read_t, NULL, lru_get_chunk_thread, NULL);
    pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
    pthread_create(&hash_t, NULL, sha256_thread, NULL);
	start_dedup_phase();
	start_rewrite_phase();
	start_filter_phase();

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num);

	assert(sync_queue_size(update_recipe_queue) == 0);
	assert(sync_queue_size(upgrade_chunk_queue) == 0);
	assert(sync_queue_size(pre_dedup_queue) == 0);
	assert(sync_queue_size(hash_queue) == 0);

	free_backup_version(jcr.bv);
	update_backup_version(jcr.new_bv);
	free_backup_version(jcr.new_bv);

	pthread_join(recipe_t, NULL);
	pthread_join(read_t, NULL);
	pthread_join(pre_dedup_t, NULL);
	pthread_join(hash_t, NULL);
	stop_dedup_phase();
	stop_rewrite_phase();
	stop_filter_phase();

	TIMER_END(1, jcr.total_time);
	puts("==== update end ====");

	close_index();
	close_container_store();
	close_recipe_store();
	pthread_mutex_destroy(&upgrade_index_lock.mutex);

	printf("job id: %" PRId32 "\n", jcr.id);
	printf("update path: %s\n", jcr.path);
	printf("number of files: %" PRId32 "\n", jcr.file_num);
	printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
	printf("total size(B): %" PRId64 "\n", jcr.data_size);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("write_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.write_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);

	char logfile[] = "log/update.log";
	FILE *fp = fopen(logfile, "a");

	/*
	 * job id,
	 * the size of backup
	 * accumulative consumed capacity,
	 * deduplication rate,
	 * rewritten rate,
	 * total container number,
	 * sparse container number,
	 * inherited container number,
	 * 4 * index overhead (4 * int)
	 * throughput,
	 */
	fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId64 " %.4f %.4f %" PRId32 " %" PRId32 " %" PRId32 " %" PRId32" %" PRId32 " %" PRId32" %" PRId32" %.2f\n",
			jcr.id,
			jcr.data_size,
			destor.stored_data_size,
			jcr.data_size != 0 ?
					(jcr.data_size - jcr.rewritten_chunk_size - jcr.unique_data_size)/(double) (jcr.data_size)
					: 0,
			jcr.data_size != 0 ? (double) (jcr.rewritten_chunk_size) / (double) (jcr.data_size) : 0,
			jcr.total_container_num,
			jcr.sparse_container_num,
			jcr.inherited_sparse_num,
			index_overhead.lookup_requests,
			index_overhead.lookup_requests_for_unique,
			index_overhead.update_requests,
			index_overhead.read_prefetching_units,
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);
}
