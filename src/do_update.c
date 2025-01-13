#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "utils/cache.h"
#include "update.h"
#include "backup.h"
#include "index/index.h"
#include "index/upgrade_cache.h"
#include "similarity.h"

#define QUEUE_SIZE 5
/* defined in index.c */
extern struct index_overhead index_overhead, upgrade_index_overhead;
extern GHashTable *upgrade_processing;
extern GHashTable *upgrade_container;

upgrade_lock_t upgrade_index_lock;
static void* sha256_thread(void* arg);
void end_update();

static void* read_recipe_thread(void *arg) {
	pthread_setname_np(pthread_self(), "read_recipe_thread");
	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
		NOTICE("Send recipe %s", r->filename);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(upgrade_recipe_queue, c);

		TIMER_BEGIN(1);
		struct chunkPointer* cps = read_next_n_chunk_pointers(jcr.bv, r->chunknum, &k);
		TIMER_END(1, jcr.read_recipe_time);
		assert(k == r->chunknum);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_BEGIN(1);

			struct chunkPointer* cp = cps + j;

			struct chunk* c = new_chunk(0);
			memcpy(&c->old_fp, &cp->fp, sizeof(fingerprint));
			assert(!memcmp(c->old_fp + 20, zero_fp, 12));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);

			sync_queue_push(upgrade_recipe_queue, c);
		}
		free(cps);

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(upgrade_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	sync_queue_term(upgrade_recipe_queue);
	return NULL;
}

static void* lru_get_chunk_thread(void *arg) {
	struct lruCache *cache;
	// if (destor.simulation_level >= SIMULATION_RESTORE)
	cache = new_lru_cache(destor.restore_cache[1], free_container,
			lookup_fingerprint_in_container);

	struct chunk* c;
	while ((c = sync_queue_pop(pre_dedup_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(upgrade_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		// if (destor.simulation_level >= SIMULATION_RESTORE) {
		struct container *con = lru_cache_lookup(cache, &c->old_fp);
		if (!con) {
			con = retrieve_container_by_id(c->id);
			lru_cache_insert(cache, con, NULL, NULL);
			jcr.read_container_num++;
		}
		struct chunk *rc = get_chunk_in_container(con, &c->old_fp);
		memcpy(rc->old_fp, c->old_fp, sizeof(fingerprint));
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

#define CONTAINER_BUFFER_SIZE 2
void* read_container_thread(void *arg) {
	pthread_setname_np(pthread_self(), "read_container");

	struct chunk *ck;
	int64_t count = get_container_count();
	struct container **buffer[CONTAINER_BUFFER_SIZE], *con;
	int bufOffset = 0, bufSize = 0;
	for (containerid id = 0; id < count; id++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		
		if (bufOffset == bufSize) {
			bufSize = (count - id) > CONTAINER_BUFFER_SIZE ? CONTAINER_BUFFER_SIZE : (count - id);
			for (int i = 0; i < bufSize; i++) {
				buffer[i] = retrieve_container_by_id(id + i);
				jcr.read_container_num++;
			}
			bufOffset = 0;
		}
		con = buffer[bufOffset++];

		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, con);
		jcr.processed_container_num++; 
	}
	sync_queue_term(upgrade_chunk_queue);
	return NULL;
}

void *reorder_dedup_thread(void *arg) {
	pthread_setname_np(pthread_self(), "reorder_dedup");
	recipeUnit_t *c;
	while ((c = sync_queue_pop(upgrade_recipe_queue))) {
		assert(jcr.container_processed);
		for (int i = 0; i < c->chunk_num; i++) {
			upgrade_index_lookup(c->cks + i);
			assert(CHECK_CHUNK((c->cks + i), CHUNK_DUPLICATE));
		}
		sync_queue_push(hash_queue, c);
	}
	sync_queue_term(hash_queue);
	return NULL;
}

static void* pre_dedup_thread(void *arg) {
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_recipe_queue);

		if (c == NULL) {
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| destor.upgrade_level == UPGRADE_LFU) {
			sync_queue_push(pre_dedup_queue, c);
			continue;
		}
	}
	sync_queue_term(pre_dedup_queue);
	return NULL;
}

static void* sha256_thread(void* arg) {
	pthread_setname_np(pthread_self(), "sha256_thread");
	// 只有计算在container内的chunk的hash, 如果不是2D, 则始终为TRUE
	int in_container = TRUE;
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_chunk_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_CONTAINER_START)) {
			in_container = TRUE;
		} else if (CHECK_CHUNK(c, CHUNK_CONTAINER_END)) {
			in_container = FALSE;
		}

		if (!in_container || IS_SIGNAL_CHUNK(c) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		if (destor.simulation_level >= SIMULATION_RESTORE) {
			jcr.hash_num++;
			if (CHECK_CHUNK(c, CHUNK_REPROCESS)) {
				memcpy(c->old_fp, c->fp, sizeof(fingerprint));
			} else {
				memcpy(c->fp, c->old_fp, sizeof(fingerprint));
			}
			sync_queue_push(hash_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		jcr.hash_num++;
		if (CHECK_CHUNK(c, CHUNK_REPROCESS)) {
			// 计算SHA1
			assert(c->id >= 0);
			SHA_CTX ctx;
			SHA1_Init(&ctx);
			SHA1_Update(&ctx, c->data, c->size);
			SHA1_Final(c->old_fp, &ctx);
		} else {
			// 计算SHA256
			assert(c->id == TEMPORARY_ID);
			SHA256_CTX ctx;
			SHA256_Init(&ctx);
			SHA256_Update(&ctx, c->data, c->size);
			SHA256_Final(c->fp, &ctx);
		}
		TIMER_END(1, jcr.hash_time);
		sync_queue_push(hash_queue, c);
	}
	return NULL;
}

static void* sha256_container(void* arg) {
	pthread_setname_np(pthread_self(), "sha256_thread");
	struct container *con;
	struct chunk *c;
	while ((con = sync_queue_pop(upgrade_chunk_queue)) != NULL) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		for (int i = 0; i < con->meta.chunk_num; i++) {
			c = con->chunks + i;
			jcr.hash_num++;
			if (destor.simulation_level >= SIMULATION_RESTORE) {
				memcpy(c->fp, c->old_fp, sizeof(fingerprint));
			} else {
				SHA256_CTX ctx;
				SHA256_Init(&ctx);
				SHA256_Update(&ctx, c->data, c->size);
				SHA256_Final(c->fp, &ctx);
			}
		}
		TIMER_END(1, jcr.hash_time);
		sync_queue_push(hash_queue, con);
	}
	sync_queue_term(hash_queue);
	return NULL;
}

void print_status() {
	fprintf(stderr, "%" PRId64 " GB, %" PRId32 " chunks, %d files, %d container, %d files pre_processed\r", 
		jcr.data_size >> 30, jcr.chunk_num, jcr.file_num, jcr.processed_container_num, jcr.pre_process_file_num);
}

void wait_jobs_done() {
	do {
		sleep(5);
		print_status();
	} while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
	print_status();
	printf("\n");
}

static void pre_process_args() {

	// CDC
	assert(destor.CDC_max_size >= destor.CDC_exp_size);
	assert(destor.CDC_exp_size >= destor.CDC_min_size);
	assert(destor.CDC_min_size > 0);

	destor.upgrade_relation_level = 2;
	destor.upgrade_cdc_level = UPGRADE_CDC_CONTAINER;
	switch (destor.upgrade_level) {
	case UPGRADE_LFU:
		destor.external_cache_size = 0; // no limit
		break;
	case UPGRADE_PFU:
		assert(destor.upgrade_external_store == INDEX_KEY_VALUE_ROCKSDB);
		destor.upgrade_reorder = 1;
		destor.upgrade_relation_level = 1;
		break;
	case UPGRADE_CONPFU:
		destor.upgrade_relation_level = 2;
		destor.upgrade_reorder = 1;
		break;
	case UPGRADE_SIMPFU:
		destor.upgrade_reorder = 1;
		destor.upgrade_similarity = 1;
		destor.upgrade_do_split_merge = 1;
		break;
	default:
		assert(0);
		break;
	}
}

void do_reorder_upgrade_container() {
	pthread_t read_t, hash_t, filter_t, recipe_t;

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	puts("==== upgrade container begin ====");
	jcr.status = JCR_STATUS_RUNNING;
	upgrade_chunk_queue = sync_queue_new(QUEUE_SIZE);
	hash_queue = sync_queue_new(QUEUE_SIZE);
	pthread_create(&read_t, NULL, read_container_thread, NULL);
	pthread_create(&hash_t, NULL, sha256_container, NULL);
	pthread_create(&filter_t, NULL, filter_thread_container, NULL);
	pthread_create(&recipe_t, NULL, pre_process_recipe_thread, NULL);

	wait_jobs_done();

	assert(sync_queue_size(upgrade_chunk_queue) == 0);
	assert(sync_queue_size(hash_queue) == 0);
	pthread_join(read_t, NULL);
	pthread_join(hash_t, NULL);
	pthread_join(filter_t, NULL);
	pthread_join(recipe_t, NULL);
	wait_append_thread();
	TIMER_END(1, jcr.pre_process_container_time);
	jcr.container_filter_time = jcr.filter_time;
	jcr.filter_time = 0;
	jcr.container_processed = 1;
}

void do_reorder_upgrade_recipe() {
	pthread_t read_t, dedup_t, filter_t;

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	puts("==== upgrade recipe begin ====");
	jcr.status = JCR_STATUS_RUNNING;
	upgrade_recipe_queue = sync_queue_new(QUEUE_SIZE);
	hash_queue = sync_queue_new(QUEUE_SIZE);
	if (destor.upgrade_similarity) {
		pthread_create(&read_t, NULL, read_similarity_recipe_thread, (void *)1);
	} else {
		pthread_create(&read_t, NULL, read_recipe_batch_thread, NULL);
	}
	pthread_create(&dedup_t, NULL, reorder_dedup_thread, NULL);
	pthread_create(&filter_t, NULL, filter_thread_recipe, NULL);
	
	wait_jobs_done();

	assert(sync_queue_size(upgrade_recipe_queue) == 0);
	assert(sync_queue_size(hash_queue) == 0);
	pthread_join(read_t, NULL);
	pthread_join(dedup_t, NULL);
	pthread_join(filter_t, NULL);
	TIMER_END(1, jcr.recipe_time);
}

void do_reorder_upgrade() {

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	switch (destor.upgrade_phase)
	{
	case 0:
		do_reorder_upgrade_container();
		do_reorder_upgrade_recipe();
		break;
	case 1:
		do_reorder_upgrade_container();
		break;
	case 2: {
		pthread_t recipe_t;
		pthread_create(&recipe_t, NULL, pre_process_recipe_thread, NULL);
		pthread_join(recipe_t, NULL);
		jcr.container_processed = 1;
		do_reorder_upgrade_recipe();
		break;
	}
	default:
		assert(0);
		break;
	}

	upgrade_recipe_meta(jcr.bv, jcr.new_bv);
	free_backup_version(jcr.bv);
	free_backup_version(jcr.new_bv);

	TIMER_END(1, jcr.total_time);
	end_update();
}

void record_args() {
	WARNING("job id: %d", jcr.id);
	WARNING("new job id: %d", jcr.new_id);
	WARNING("backup path: %s", jcr.bv->path);
	WARNING("new backup path: %s", jcr.new_bv->path);
	WARNING("update to: %s", jcr.path);
	WARNING("upgrade_level %d", destor.upgrade_level);
	WARNING("upgrade_phase %d", destor.upgrade_phase);
	WARNING("upgrade_relation_level %d", destor.upgrade_relation_level);
	WARNING("upgrade_cdc_level %d", destor.upgrade_cdc_level);
	WARNING("upgrade_similarity %d", destor.upgrade_similarity);
	WARNING("upgrade_reorder %d", destor.upgrade_reorder);
	WARNING("upgrade_do_split_merge %d", destor.upgrade_do_split_merge);
	WARNING("CDC %d %d %d", destor.CDC_min_size, destor.CDC_exp_size, destor.CDC_max_size);
	WARNING("cache size %d %d", destor.index_cache_size, destor.external_cache_size);
	WARNING("Simulation level %d", destor.simulation_level);
	WARNING("index_key_value_store %d", destor.index_key_value_store);
	WARNING("upgrade_external_store %d", destor.upgrade_external_store);
	WARNING("direct_reads %d", destor.direct_reads);
}

void do_update(int revision, char *path) {
	pthread_setname_np(pthread_self(), "main");

	pre_process_args();

	init_recipe_store();
	init_container_store();
	init_index();

	init_update_jcr(revision, path);
	pthread_mutex_init(&upgrade_index_lock.mutex, NULL);
	record_args();

	if (destor.upgrade_reorder) {
		do_reorder_upgrade();
		return;
	}
	
	upgrade_recipe_queue = sync_queue_new(QUEUE_SIZE);
	upgrade_chunk_queue = sync_queue_new(QUEUE_SIZE);
	pre_dedup_queue = sync_queue_new(QUEUE_SIZE);
	hash_queue = sync_queue_new(QUEUE_SIZE);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== update begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, pre_dedup_t, hash_t;
	switch (destor.upgrade_level)
	{
	case UPGRADE_LFU:
		pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	default:
		assert(0);
	}

	if (destor.upgrade_level == UPGRADE_LFU) {
		start_dedup_phase();
		start_rewrite_phase();
	}
	start_filter_phase();

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        print_status();
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
	print_status();

	assert(sync_queue_size(upgrade_recipe_queue) == 0);
	assert(sync_queue_size(upgrade_chunk_queue) == 0);
	assert(sync_queue_size(pre_dedup_queue) == 0);
	assert(sync_queue_size(hash_queue) == 0);

	free_backup_version(jcr.bv);
	update_backup_version(jcr.new_bv);
	free_backup_version(jcr.new_bv);

	pthread_join(recipe_t, NULL);
	pthread_join(pre_dedup_t, NULL);
	pthread_join(read_t, NULL);
	pthread_join(hash_t, NULL);
	if (destor.upgrade_level == UPGRADE_LFU) {
		stop_dedup_phase();
		stop_rewrite_phase();
	}
	stop_filter_phase();

	TIMER_END(1, jcr.total_time);
	end_update();
}

void end_update() {
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
	
	printf("1. container_time: %.3fs\n", jcr.pre_process_container_time / 1000000);
	printf("----- read_chunk_time:\t%.3fs\n", jcr.read_chunk_time / 1000000);
	printf("----- hash_time:\t%.3fs\n", jcr.hash_time / 1000000);
	printf("----- filter_time:\t%.3fs\n", jcr.container_filter_time / 1000000);
	printf("----- append_time:\t%.3fs\n", jcr.write_time / 1000000);
	printf("----- pre_recipe_time:\t%.3fs\n", jcr.pre_process_recipe_time / 1000000);
	printf("2. recipe_time: %.3fs\n", jcr.recipe_time / 1000000);
	printf("----- read_recipe_time:\t%.3fs\n", jcr.read_recipe_time / 1000000);
	printf("----- dedup_time:\t%.3fs\n", jcr.pre_dedup_time / 1000000);
	printf("----- filter_time:\t%.3fs\n", jcr.filter_time / 1000000);

	printf("memory_cache_time:\t%.3fs\n", jcr.memory_cache_time / 1000000);
	printf("external_cache_time:\t%.3fs\n", jcr.external_cache_time / 1000000);
	printf("external_cache_lookup:\t%" PRId32 "\n", upgrade_index_overhead.kvstore_lookup_requests);

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
			index_overhead.kvstore_lookup_requests,
			index_overhead.lookup_requests_for_unique,
			index_overhead.kvstore_update_requests,
			index_overhead.read_prefetching_units,
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);

	fp = stdout;
	fprintf(fp, "========== jcr_result ==========\n");
	print_jcr_result(fp);
	fprintf(fp, "========== upgrade_index_overhead ==========\n");
	print_index_overhead(fp, &upgrade_index_overhead);
	fprintf(fp, "========== index_overhead ==========\n");
	print_index_overhead(fp, &index_overhead);
	// fclose(fp);

}
