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
extern GHashTable *upgrade_processing;

upgrade_lock_t upgrade_index_lock;

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	DECLARE_TIME_RECORDER("read_recipe_thread");
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		BEGIN_TIME_RECORD;
		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
		END_TIME_RECORD;

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(upgrade_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			BEGIN_TIME_RECORD
			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);
			END_TIME_RECORD

			struct chunk* c = new_chunk(0);
			memcpy(&c->old_fp, &cp->fp, sizeof(fingerprint));
			assert(!memcmp(c->old_fp + 20, zero_fp, 12));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);

			sync_queue_push(upgrade_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(upgrade_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	FINISH_TIME_RECORD
	sync_queue_term(upgrade_recipe_queue);
	return NULL;
}

#define FEATURE_NUM 4
typedef containerid feature;
typedef feature (*feature_func)(containerid);
feature feature1(containerid x) { return x * 3 + 5; }
feature feature2(containerid x) { return x * 7 + 11; }
feature feature3(containerid x) { return x * 13 + 17; }
feature feature4(containerid x) { return x * 19 + 23; }
feature_func feature_func_list[FEATURE_NUM] = {feature1, feature2, feature3, feature4};
struct featureList {
	feature feature;
	size_t count;
	size_t max_count;
	containerid *recipeIDList;
};

void free_featureList(gpointer data) {
	struct featureList *list = data;
	free(list->recipeIDList);
	free(list);
}

int compare_container_id(void *a, void *b) {
	return *(containerid*) a == *(containerid*) b;
}

static void* read_similarity_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	DECLARE_TIME_RECORDER("read_recipe_thread");
	struct fileRecipeMeta **recipeList = malloc(sizeof(struct fileRecipeMeta *) * jcr.bv->number_of_files);
	struct chunkPointer **chunkList = malloc(sizeof(struct chunkPointer *) * jcr.bv->number_of_files);
	// container id -> featureList[ recipe id ]
	GHashTable *featureTable = g_hash_table_new_full(g_int64_hash, g_int64_equal, free_featureList, NULL);

	// read all recipes and calculate features
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	BEGIN_TIME_RECORD;
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		feature features[4] = { LLONG_MAX, LLONG_MAX, LLONG_MAX, LLONG_MAX };
		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
		struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, r->chunknum, &k);
		assert(r->chunknum == k);
		recipeList[i] = r;
		chunkList[i] = cp;

		for (j = 0; j < r->chunknum; j++) {
			for (k = 0; k < FEATURE_NUM; k++) {
				features[k] = MIN(features[k], feature_func_list[k](cp[j].id));
			}
		}
		for (k = 0; k < FEATURE_NUM; k++) {
			struct featureList *list = g_hash_table_lookup(featureTable, &features[k]);
			if (!list) {
				list = malloc(sizeof(struct featureList));
				list->count = 0;
				list->max_count = 1;
				list->recipeIDList = malloc(sizeof(containerid) * list->max_count);
				list->feature = features[k];
				g_hash_table_insert(featureTable, &list->feature, list);
			} else if (list->count >= list->max_count) {
				list->max_count *= 2;
				list->recipeIDList = realloc(list->recipeIDList, sizeof(containerid) * list->max_count);
			}
			list->recipeIDList[list->count++] = i;
		}
	}
	TIMER_END(1, jcr.read_recipe_time);
	END_TIME_RECORD;

	// send recipes
	struct lruCache *lru = new_lru_cache(destor.index_cache_size, free, compare_container_id);
	feature featuresInLRU[FEATURE_NUM] = { LLONG_MAX, LLONG_MAX, LLONG_MAX, LLONG_MAX };
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		struct fileRecipeMeta *r = recipeList[i];
		struct chunkPointer *cp = chunkList[i];

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);
		sync_queue_push(upgrade_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			struct chunk* c = new_chunk(0);
			memcpy(&c->old_fp, &cp[j].fp, sizeof(fingerprint));
			assert(!memcmp(c->old_fp + 20, zero_fp, 12));
			c->size = cp[j].size;
			c->id = cp[j].id;
			sync_queue_push(upgrade_recipe_queue, c);

			// simulate LRU
			// 如果找到, 会自动将其移到头部
			containerid *id_p = lru_cache_lookup(lru, &c->id);
			if (id_p) continue;
			if (lru->size == 0) {
				for (k = 0; k < FEATURE_NUM; k++) {
					assert(featuresInLRU[k] == LLONG_MAX);
					featuresInLRU[k] = feature_func_list[k](c->id);
				}
				id_p = malloc(sizeof(containerid));
				*id_p = c->id;
				lru_cache_insert(lru, id_p, NULL, NULL);
				continue;
			}

			// 插入新的containerid
			assert(lru->elem_queue_tail);
			containerid lastID = *(containerid *)lru->elem_queue_tail->data;
			id_p = malloc(sizeof(containerid));
			*id_p = c->id;
			for (k = 0; k < FEATURE_NUM; k++) {
				feature_func ffunc = feature_func_list[k];
				feature f = ffunc(c->id);
				assert(f != featuresInLRU[k]);
				if (f < featuresInLRU[k]) {
					featuresInLRU[k] = f;
				} else if (lru_cache_is_full(lru) && ffunc(lastID) == featuresInLRU[k]) {
					// 如果是最后一个, 需要重新计算特征
					feature best = f;
					GList* elem = g_list_first(lru->elem_queue);
					// 最后一个不参与
					while (elem && elem->next) {
						best = MIN(best, ffunc(*(containerid *)elem->data));
						elem = g_list_next(elem);
					}
					featuresInLRU[k] = best;
				}
			}
			lru_cache_insert(lru, id_p, NULL, NULL);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(upgrade_recipe_queue, c);

		free_file_recipe_meta(r);
		free(cp);
	}

	FINISH_TIME_RECORD
	sync_queue_term(upgrade_recipe_queue);
	free_lru_cache(lru);
	g_hash_table_destroy(featureTable);
	free(recipeList);
	free(chunkList);
	return NULL;
}

static void* lru_get_chunk_thread(void *arg) {
	struct lruCache *cache;
	// if (destor.simulation_level >= SIMULATION_RESTORE)
	cache = new_lru_cache(destor.restore_cache[1], free_container,
			lookup_fingerprint_in_container);

	DECLARE_TIME_RECORDER("lru_get_chunk_thread");
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
			BEGIN_TIME_RECORD;
			con = retrieve_container_by_id(c->id);
			END_TIME_RECORD;
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
	FINISH_TIME_RECORD;

	sync_queue_term(upgrade_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

static void* lru_get_chunk_thread_2D(void *arg) {
	DECLARE_TIME_RECORDER("lru_get_chunk_thread");
	struct chunk *c, *ck; // c: get from queue, ck: temp chunk
	while ((c = sync_queue_pop(pre_dedup_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(upgrade_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		// 已经发送过的container不再发送
		assert(c->id >= 0);
		DEBUG("lru_get_chunk_thread_2D %ld", c->id);
		BEGIN_TIME_RECORD;
		struct container *con = retrieve_container_by_id(c->id);
		END_TIME_RECORD
		assert(con);

		// send container
		ck = new_chunk(0);
		SET_CHUNK(ck, CHUNK_CONTAINER_START);
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, ck);
		TIMER_BEGIN(1);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, con->meta.map);
		while(g_hash_table_iter_next(&iter, &key, &value)){
			ck = get_chunk_in_container(con, key);
			assert(ck);
			memcpy(ck->old_fp, ck->fp, sizeof(fingerprint));
			ck->id = TEMPORARY_ID;
			TIMER_END(1, jcr.read_chunk_time);
			sync_queue_push(upgrade_chunk_queue, ck);
			TIMER_BEGIN(1);
		}

		ck = new_chunk(0);
		ck->id = c->id;
		SET_CHUNK(ck, CHUNK_CONTAINER_END);
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, ck);
		TIMER_BEGIN(1);

		free_container(con);
		jcr.read_container_num++;
			
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, c);

	}

	FINISH_TIME_RECORD
	sync_queue_term(upgrade_chunk_queue);
	return NULL;
}


static void* pre_dedup_thread(void *arg) {
	DECLARE_TIME_RECORDER("pre_dedup_thread");
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_recipe_queue);

		if (c == NULL) {
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(pre_dedup_queue, c);
			continue;
		}

		if (destor.upgrade_level != UPGRADE_NAIVE) {
			/* Each duplicate chunk will be marked. */
			pthread_mutex_lock(&upgrade_index_lock.mutex);
			// while (upgrade_index_lookup(c) == 0) { // 目前永远是1, 所以不用管cond
			// 	pthread_cond_wait(&upgrade_index_lock.cond, &upgrade_index_lock.mutex);
			// }
			BEGIN_TIME_RECORD
			if (destor.upgrade_level == UPGRADE_2D_RELATION || destor.upgrade_level == UPGRADE_SIMILARITY) {
				if (g_hash_table_lookup(upgrade_processing, &c->id)) {
					// container正在处理中, 标记为duplicate, c->id为TEMPORARY_ID
					DEBUG("container processing: %ld", c->id);
					SET_CHUNK(c, CHUNK_DUPLICATE);
					c->id = TEMPORARY_ID;
					jcr.sync_buffer_num++;
				} else {
					upgrade_index_lookup(c);
					if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
						// 非重复块需要在下一阶段开始处理, 这个不能放到下面的阶段, 必须在同一锁内
						int64_t *id = malloc(sizeof(int64_t));
						*id = c->id;
						g_hash_table_insert(upgrade_processing, id, "1");
					}
				}
			} else if (destor.upgrade_level == UPGRADE_1D_RELATION) {
				upgrade_index_lookup(c);
			} else {
				// Not Implemented
				assert(0);
			}
			END_TIME_RECORD
			pthread_mutex_unlock(&upgrade_index_lock.mutex);
			
		}

		sync_queue_push(pre_dedup_queue, c);
	}
	FINISH_TIME_RECORD
	sync_queue_term(pre_dedup_queue);

	return NULL;
}

static void* sha256_thread(void* arg) {
	DECLARE_TIME_RECORDER("sha256_thread");
	// char code[41];
	// 只有计算在container内的chunk的hash, 如果不是2D, 则始终为TRUE
	int in_container = destor.upgrade_level == UPGRADE_2D_RELATION ? FALSE : TRUE;
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

		BEGIN_TIME_RECORD

		assert(c->id == TEMPORARY_ID);
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		jcr.hash_num++;
		SHA256_CTX ctx;
		SHA256_Init(&ctx);
		SHA256_Update(&ctx, c->data, c->size);
		SHA256_Final(c->fp, &ctx);
		TIMER_END(1, jcr.hash_time);

		END_TIME_RECORD

		// hash2code(c->fp, code);
		// code[40] = 0;
		// VERBOSE("Update hash phase: %ldth chunk identified by %s", chunk_num++, code);

		sync_queue_push(hash_queue, c);
	}
	FINISH_TIME_RECORD
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
	destor_log(DESTOR_NOTICE, "upgrade_level %d", destor.upgrade_level);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "new backup path: %s", jcr.new_bv->path);
	destor_log(DESTOR_NOTICE, "update to: %s", jcr.path);

	upgrade_recipe_queue = sync_queue_new(100);
	upgrade_chunk_queue = sync_queue_new(100);
	pre_dedup_queue = sync_queue_new(100);
	hash_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== update begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, pre_dedup_t, hash_t;
	switch (destor.upgrade_level)
	{
	case UPGRADE_NAIVE:
	case UPGRADE_1D_RELATION:
		pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	case UPGRADE_2D_RELATION:
		pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread_2D, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	case UPGRADE_SIMILARITY:
		pthread_create(&recipe_t, NULL, read_similarity_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread_2D, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	default:
		assert(0);
	}

	if (destor.upgrade_level == UPGRADE_NAIVE || destor.upgrade_level == UPGRADE_1D_RELATION) {
		start_dedup_phase();
		start_rewrite_phase();
	}
	start_filter_phase();

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size, jcr.chunk_num, jcr.file_num);

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
	if (destor.upgrade_level == UPGRADE_NAIVE || destor.upgrade_level == UPGRADE_1D_RELATION) {
		stop_dedup_phase();
		stop_rewrite_phase();
	}
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
	printf("pre_dedup_time : %.3fs, %.2fMB/s\n", jcr.pre_dedup_time / 1000000,
			jcr.data_size * 1000000 / jcr.pre_dedup_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("hash_time : %.3fs, %.2fMB/s\n", jcr.hash_time / 1000000,
			jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024);
	printf("dedup_time : %.3fs, %.2fMB/s\n", jcr.dedup_time / 1000000,
			jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024);
	printf("filter_time : %.3fs, %.2fMB/s\n",
			jcr.filter_time / 1000000,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024);
	printf("append_thread_time : %.3fs, %.2fMB/s\n",
			jcr.write_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024);

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
	fprintf(fp, "%u %u %u %u\n", jcr.sql_insert_all, jcr.sql_insert, jcr.sql_fetch, jcr.sql_fetch_buffered);
	fprintf(fp, "%u %u %u\n", jcr.read_container_num, jcr.hash_num, jcr.sync_buffer_num);
	print_index_overhead(fp, &upgrade_index_overhead);
	fprintf(fp, "\n");
	print_index_overhead(fp, &index_overhead);
	fprintf(fp, "\n");
	// fclose(fp);

}
