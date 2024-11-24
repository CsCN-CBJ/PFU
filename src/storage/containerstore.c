#include "containerstore.h"
#include "../utils/serial.h"
#include "../utils/sync_queue.h"
#include "../jcr.h"
#include "../destor.h"
#include "db.h"

static int64_t container_count = 0;
static FILE *old_fp, *new_fp;
/* Control the concurrent accesses to fp. */
static pthread_mutex_t old_mutex, new_mutex;

static pthread_t append_t;

static SyncQueue* container_buffer;

struct metaEntry {
	int32_t off;
	int32_t len;
	fingerprint fp;
};

/*
 * We must ensure a container is either in the buffer or written to disks.
 */
static void* append_thread(void *arg) {

	while (1) {
		struct container *c = sync_queue_get_top(container_buffer);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		write_container(c);

		TIMER_END(1, jcr.write_time);

		sync_queue_pop(container_buffer);

		free_container(c);
	}

	return NULL;
}

void init_container_store() {
	/**
	 * DESTOR_UPDATE: read container.pool, open container.pool_new
	 * DESTOR_NEW_RESTORE: read container.pool_new
	 * default: open container.pool
	*/

	sds containerfile = sdsdup(destor.working_directory);
	containerfile = sdscat(containerfile, "/container.pool");
	if (job == DESTOR_NEW_RESTORE) {
		containerfile = sdscat(containerfile, "_new");
	}

	if (job == DESTOR_UPDATE) {
		// 确保不修改原始文件
		old_fp = fopen(containerfile, "r");
		assert(old_fp);
		fread(&container_count, 8, 1, old_fp);
	} else {
		if ((old_fp = fopen(containerfile, "r+"))) {
			fread(&container_count, 8, 1, old_fp);
		} else if (!(old_fp = fopen(containerfile, "w+"))) {
			perror("Can not create container.pool for read and write because");
			exit(1);
		}
	}

	if (job == DESTOR_UPDATE) {
		containerfile = sdscat(containerfile, "_new");
		container_count = 0;
		if (!(new_fp = fopen(containerfile, "w+"))) {
			perror("Can not create container.pool_new for read and write because");
			exit(1);
		}
	}

	sdsfree(containerfile);

	container_buffer = sync_queue_new(25);

	pthread_mutex_init(&old_mutex, NULL);
	pthread_mutex_init(&new_mutex, NULL);

	pthread_create(&append_t, NULL, append_thread, NULL);

	init_upgrade_index_store();
    NOTICE("Init container store successfully");
}

void close_container_store() {
	FILE *fp = job == DESTOR_UPDATE ? new_fp : old_fp;

	sync_queue_term(container_buffer);

	pthread_join(append_t, NULL);
	NOTICE("append phase stops successfully!");

	fseek(fp, 0, SEEK_SET);
	fwrite(&container_count, sizeof(container_count), 1, fp);

	fclose(fp);
	if (job == DESTOR_UPDATE) {
		fclose(old_fp);
	}
	fp = NULL;

	pthread_mutex_destroy(&old_mutex);
	pthread_mutex_destroy(&new_mutex);

	close_upgrade_index_store();
}

static void init_container_meta(struct containerMeta *meta) {
	meta->chunk_num = 0;
	meta->data_size = 0;
	meta->id = TEMPORARY_ID;
	meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
			free);
}

/*
 * For backup.
 */
struct container* create_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	if (destor.simulation_level < SIMULATION_APPEND)
		c->data = calloc(1, CONTAINER_SIZE);
	else
		c->data = 0;

	c->fp_size = WRITE_FP_SZ;

	init_container_meta(&c->meta);
	c->meta.id = container_count++;
	return c;
}

inline containerid get_container_id(struct container* c) {
	return c->meta.id;
}

void write_container_async(struct container* c) {
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	sync_queue_push(container_buffer, c);
}

/*
 * Called by Append phase
 */
void write_container(struct container* c) {
	FILE *fp = job == DESTOR_UPDATE ? new_fp : old_fp;
	pthread_mutex_t *mutex = job == DESTOR_UPDATE ? &new_mutex : &old_mutex;

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	VERBOSE("Append phase: Writing container %lld of %d chunks", c->meta.id,
			c->meta.chunk_num);

	if (destor.simulation_level < SIMULATION_APPEND) {

		unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
		ser_declare;
		ser_begin(cur, CONTAINER_META_SIZE);
		ser_int64(c->meta.id);
		ser_int32(c->meta.chunk_num);
		ser_int32(c->meta.data_size);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, c->meta.map);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			struct metaEntry *me = (struct metaEntry *) value;
			ser_bytes(&me->fp, c->fp_size);
			ser_bytes(&me->len, sizeof(int32_t));
			ser_bytes(&me->off, sizeof(int32_t));
		}

		ser_end(cur, CONTAINER_META_SIZE);

		pthread_mutex_lock(mutex);

		if (fseek(fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(c->data, CONTAINER_SIZE, 1, fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(mutex);
	} else {
		char buf[CONTAINER_META_SIZE];
		memset(buf, 0, CONTAINER_META_SIZE);

		ser_declare;
		ser_begin(buf, CONTAINER_META_SIZE);
		ser_int64(c->meta.id);
		ser_int32(c->meta.chunk_num);
		ser_int32(c->meta.data_size);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, c->meta.map);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			struct metaEntry *me = (struct metaEntry *) value;
			ser_bytes(&me->fp, c->fp_size);
			ser_bytes(&me->len, sizeof(int32_t));
			ser_bytes(&me->off, sizeof(int32_t));
		}

		ser_end(buf, CONTAINER_META_SIZE);

		pthread_mutex_lock(mutex);

		if(fseek(fp, c->meta.id * CONTAINER_META_SIZE + 8, SEEK_SET) != 0){
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(buf, CONTAINER_META_SIZE, 1, fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(mutex);
	}

}

struct container* _retrieve_container_by_id(containerid id, FILE *fp) {
	pthread_mutex_t *mutex = fp == new_fp ? &new_mutex : &old_mutex;
	struct container *c = (struct container*) malloc(sizeof(struct container));
	c->fp_size = fp == new_fp ? sizeof(fingerprint) : READ_CONTAINER_SZ;

	init_container_meta(&c->meta);

	unsigned char *cur = 0;
	if (destor.simulation_level >= SIMULATION_RESTORE) {
		c->data = malloc(CONTAINER_META_SIZE);

		pthread_mutex_lock(mutex);

		if (destor.simulation_level >= SIMULATION_APPEND)
			fseek(fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
		else
			fseek(fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
			SEEK_SET);

		fread(c->data, CONTAINER_META_SIZE, 1, fp);

		pthread_mutex_unlock(mutex);

		cur = c->data;
	} else {
		c->data = malloc(CONTAINER_SIZE);

		pthread_mutex_lock(mutex);

		fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);
		fread(c->data, CONTAINER_SIZE, 1, fp);

		pthread_mutex_unlock(mutex);

		cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
	}

	unser_declare;
	unser_begin(cur, CONTAINER_META_SIZE);

	unser_int64(c->meta.id);
	unser_int32(c->meta.chunk_num);
	unser_int32(c->meta.data_size);

	if(c->meta.id != id){
		WARNING("expect %lld, but read %lld", id, c->meta.id);
		fprintf(stderr, "expect %lld, but read %lld", id, c->meta.id);
		assert(c->meta.id == id);
	}

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memset(&me->fp, 0, sizeof(fingerprint));
		unser_bytes(&me->fp, c->fp_size);
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(c->meta.map, &me->fp, me);
	}

	unser_end(cur, CONTAINER_META_SIZE);

	if (destor.simulation_level >= SIMULATION_RESTORE) {
		free(c->data);
		c->data = 0;
	}

	return c;
}

struct container* retrieve_container_by_id(containerid id) {
	return _retrieve_container_by_id(id, old_fp);
}

struct container* retrieve_new_container_by_id(containerid id) {
	return _retrieve_container_by_id(id, new_fp);
}

static struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct containerMeta* retrieve_container_meta_by_id(containerid id) {
	FILE *fp = job == DESTOR_UPDATE ? new_fp : old_fp;
	pthread_mutex_t *mutex = fp == new_fp ? &new_mutex : &old_mutex;
	struct containerMeta* cm = NULL;

	/* First, we find it in the buffer */
	cm = sync_queue_find(container_buffer, container_check_id, &id,
			container_meta_duplicate);

	if (cm)
		return cm;

	cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_container_meta(cm);

	unsigned char buf[CONTAINER_META_SIZE];

	pthread_mutex_lock(mutex);

	if (destor.simulation_level >= SIMULATION_APPEND)
		fseek(fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
	else
		fseek(fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
		SEEK_SET);

	fread(buf, CONTAINER_META_SIZE, 1, fp);

	pthread_mutex_unlock(mutex);

	unser_declare;
	unser_begin(buf, CONTAINER_META_SIZE);

	unser_int64(cm->id);
	unser_int32(cm->chunk_num);
	unser_int32(cm->data_size);

	if(cm->id != id){
		WARNING("expect %lld, but read %lld", id, cm->id);
		fprintf(stderr, "expect %lld, but read %lld", id, cm->id);
		assert(cm->id == id);
	}

	int i;
	for (i = 0; i < cm->chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memset(&me->fp, 0, sizeof(fingerprint));
		unser_bytes(&me->fp, READ_FP_META_SZ);
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(cm->map, &me->fp, me);
	}

	return cm;
}

static struct metaEntry* get_metaentry_in_container_meta(
		struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct chunk* get_chunk_in_container(struct container* c, fingerprint *fp) {
	struct metaEntry* me = get_metaentry_in_container_meta(&c->meta, fp);

	assert(me);

	struct chunk* ck = new_chunk(me->len);

	if (destor.simulation_level < SIMULATION_RESTORE)
		memcpy(ck->data, c->data + me->off, me->len);

	ck->size = me->len;
	ck->id = c->meta.id;
	memcpy(&ck->fp, fp, sizeof(fingerprint));

	return ck;
}

int container_overflow(struct container* c, int32_t size) {
	assert(c->fp_size == 20 || c->fp_size == 32);
	if (c->meta.data_size + size > CONTAINER_SIZE - CONTAINER_META_SIZE)
		return 1;
	/*
	 * 28 is the size of metaEntry.
	 * 16 is in struct metaEntry, see write_container
	 */
	if ((c->meta.chunk_num + 1) * (sizeof(struct metaEntry) - sizeof(fingerprint) + c->fp_size) + 16 > CONTAINER_META_SIZE)
		return 1;
	return 0;
}

/*
 * For backup.
 * return 1 indicates success.
 * return 0 indicates fail.
 */
int add_chunk_to_container(struct container* c, struct chunk* ck) {
	assert(!container_overflow(c, ck->size));
	if (g_hash_table_contains(c->meta.map, &ck->fp)) {
		NOTICE("Writing a chunk already in the container buffer!");
		ck->id = c->meta.id;
		return 0;
	}

	struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
	memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
	me->len = ck->size;
	me->off = c->meta.data_size;

	g_hash_table_insert(c->meta.map, &me->fp, me);
	c->meta.chunk_num++;

	if (destor.simulation_level < SIMULATION_APPEND)
		memcpy(c->data + c->meta.data_size, ck->data, ck->size);

	c->meta.data_size += ck->size;

	ck->id = c->meta.id;

	return 1;
}

void free_container_meta(struct containerMeta* cm) {
	g_hash_table_destroy(cm->map);
	free(cm);
}

void free_container(struct container* c) {
	g_hash_table_destroy(c->meta.map);
	if (c->data)
		free(c->data);
	free(c);
}

int container_empty(struct container* c) {
	return c->meta.chunk_num == 0 ? 1 : 0;
}

/*
 * Return 0 if doesn't exist.
 */
int lookup_fingerprint_in_container_meta(struct containerMeta* cm,
		fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp) == NULL ? 0 : 1;
}

int lookup_fingerprint_in_container(struct container* c, fingerprint *fp) {
	return lookup_fingerprint_in_container_meta(&c->meta, fp);
}

gint g_container_cmp_desc(struct container* c1, struct container* c2,
		gpointer user_data) {
	return g_container_meta_cmp_desc(&c1->meta, &c2->meta, user_data);
}

gint g_container_meta_cmp_desc(struct containerMeta* cm1,
		struct containerMeta* cm2, gpointer user_data) {
	return cm2->id - cm1->id;
}

int container_check_id(struct container* c, containerid* id) {
	return container_meta_check_id(&c->meta, id);
}

int container_meta_check_id(struct containerMeta* cm, containerid* id) {
	return cm->id == *id ? 1 : 0;
}

/*
 * foreach the fingerprints in the container.
 * Apply the 'func' for each fingerprint.
 */
void container_meta_foreach(struct containerMeta* cm, void (*func)(fingerprint*, void*), void* data){
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, cm->map);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		func(key, data);
	}
}

// key: id
// value: upgrade_index_container(GHashTable<old_fp, upgrade_index_value_t>)
static GHashTable* upgrade_index_store;

void init_upgrade_index_store() {
	upgrade_index_store = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, g_hash_table_destroy);
}

void close_upgrade_index_store() {
	g_hash_table_destroy(upgrade_index_store);
}

void write_upgrade_index_container(GHashTable* c, int64_t id) {
	int64_t *cid = (int64_t*) malloc(sizeof(int64_t));
	*cid = id;
	g_hash_table_insert(upgrade_index_store, cid, c);
}

GHashTable* retrieve_upgrade_index_container_by_id(int64_t id) {
	return g_hash_table_lookup(upgrade_index_store, &id);
}

int64_t get_container_count() {
	int64_t count = 0;
	pthread_mutex_lock(&old_mutex);
	fseek(old_fp, 0, SEEK_SET);
	fread(&count, 8, 1, old_fp);
	pthread_mutex_unlock(&old_mutex);
	return count;
}
