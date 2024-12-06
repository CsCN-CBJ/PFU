/*
 * containerstore.h
 *
 *  Created on: Nov 11, 2013
 *      Author: fumin
 */

#ifndef CONTAINERSTORE_H_
#define CONTAINERSTORE_H_

#include "../destor.h"

#define CONTAINER_SIZE (4194304ll) //4MB
#define CONTAINER_META_SIZE (32768ll) //32KB
#define CONTAINER_HEAD 16
#define CONTAINER_META_ENTRY 28
#define MAX_META_PER_CONTAINER (CONTAINER_META_SIZE / CONTAINER_META_ENTRY + 1)

struct containerMeta {
	containerid id;
	int32_t data_size;
	int32_t chunk_num;

	/* Map fingerprints to chunk offsets. */
	GHashTable *map;
};

struct container {
	struct containerMeta meta;
	unsigned char *data;
	uint32_t fp_size;
	struct chunk *chunks;
};

void init_container_store();
void wait_append_thread();
void close_container_store();

struct container* create_container();

void write_container(struct container*);
void write_container_async(struct container*);
struct container* retrieve_container_by_id(containerid);
struct container* retrieve_new_container_by_id(containerid);
struct containerMeta* retrieve_container_meta_by_id(containerid);
struct containerMeta* retrieve_container_meta_by_id_async(containerid);

struct chunk* get_chunk_in_container(struct container*, fingerprint*);
int add_chunk_to_container(struct container*, struct chunk*);
int container_overflow(struct container*, int32_t size);
void free_container(struct container*);
void free_container_meta(struct containerMeta*);
containerid get_container_id(struct container* c);
int container_empty(struct container* c);

gint g_container_cmp_desc(struct container*, struct container*, gpointer);
gint g_container_meta_cmp_desc(struct containerMeta*, struct containerMeta*,
		gpointer);

int lookup_fingerprint_in_container(struct container*, fingerprint *);
int lookup_fingerprint_in_container_meta(struct containerMeta*, fingerprint *);
int container_check_id(struct container*, containerid*);
int container_meta_check_id(struct containerMeta*, containerid*);

void container_meta_foreach(struct containerMeta* cm, void (*func)(fingerprint*, void*), void* data);

// upgrade
void init_upgrade_index_store();
void close_upgrade_index_store();
void write_upgrade_index_container(GHashTable* c, int64_t id);
GHashTable* retrieve_upgrade_index_container_by_id(int64_t id);
int64_t get_container_count();

#endif /* CONTAINERSTORE_H_ */
