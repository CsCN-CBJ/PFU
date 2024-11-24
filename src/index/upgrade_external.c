#include "upgrade_external.h"
#include "../storage/db.h"
#include "../utils/lru_cache.h"
#include "../storage/containerstore.h"

void (*upgrade_external_cache_insert)(containerid id, GHashTable *htb);
int (*upgrade_external_cache_prefetch)(containerid id);

// 第一个留作指示size
#define MAX_CHUNK_PER_CONTAINER 1200
static lruHashMap_t *external_cache_htb;
FILE *external_cache_file;
upgrade_index_kv_t *external_file_buffer;

void upgrade_external_cache_insert_htb(containerid id, GHashTable *htb);
void upgrade_external_cache_insert_DB(containerid id, GHashTable *htb);
void upgrade_external_cache_insert_file(containerid id, GHashTable *htb);

int upgrade_external_cache_prefetch_htb(containerid id);
int upgrade_external_cache_prefetch_DB(containerid id);
int upgrade_external_cache_prefetch_file(containerid id);

void init_upgrade_external_cache() {
    switch (destor.index_key_value_store)
    {
    case INDEX_KEY_VALUE_HTABLE:
        if (destor.fake_containers) {
            external_cache_htb = new_lru_hashmap(destor.external_cache_size, NULL, g_int64_hash, g_int64_equal);
        } else {
            external_cache_htb = new_lru_hashmap(destor.external_cache_size, g_hash_table_destroy, g_int64_hash, g_int64_equal);
        }
        upgrade_external_cache_insert = upgrade_external_cache_insert_htb;
        upgrade_external_cache_prefetch = upgrade_external_cache_prefetch_htb;
        break;
    case INDEX_KEY_VALUE_MYSQL:
        assert(0);
        break;
    case INDEX_KEY_VALUE_ROR:
        initDB(DB_UPGRADE);
        upgrade_external_cache_insert = upgrade_external_cache_insert_DB;
        upgrade_external_cache_prefetch = upgrade_external_cache_prefetch_DB;
        break;
    case INDEX_KEY_VALUE_FILE: {
        sds path = sdsdup(destor.working_directory);
        path = sdscat(path, "/upgrade_external_cache");
        external_cache_file = fopen(path, "w+");
        sdsfree(path);
        external_file_buffer = malloc(sizeof(upgrade_index_kv_t) * MAX_CHUNK_PER_CONTAINER);

        upgrade_external_cache_insert = upgrade_external_cache_insert_file;
        upgrade_external_cache_prefetch = upgrade_external_cache_prefetch_file;
        break;
    }
    default:
        assert(0);
    }
}

void close_upgrade_external_cache() {
    switch (destor.index_key_value_store)
    {
    case INDEX_KEY_VALUE_HTABLE:
        // pass
        break;
    case INDEX_KEY_VALUE_MYSQL:
        assert(0);
        break;
    case INDEX_KEY_VALUE_ROR:
        closeDB(DB_UPGRADE);
        break;
    case INDEX_KEY_VALUE_FILE:
        fclose(external_cache_file);
        free(external_file_buffer);
        break;
    default:
        break;
    }
}

/**
 * prefetch external cache
 * return 0 if not found
*/
int upgrade_external_cache_prefetch_file(containerid id) {
    assert(MAX_CHUNK_PER_CONTAINER > CONTAINER_META_SIZE / 28); // min sizof(struct metaEntry) = 28
    fseek(external_cache_file, id * sizeof(upgrade_index_kv_t) * MAX_CHUNK_PER_CONTAINER, SEEK_SET);
    size_t read_size = fread(external_file_buffer, sizeof(upgrade_index_kv_t), MAX_CHUNK_PER_CONTAINER, external_cache_file);
    if (read_size == 0) {
        return 0;
    }
    assert(read_size == MAX_CHUNK_PER_CONTAINER); // 会先处理完再读, 所以一定能读到MAX_CHUNK_PER_CONTAINER

    upgrade_index_kv_t *kv = external_file_buffer;
    assert(memcmp(&kv->old_fp, &id, sizeof(containerid)) == 0);
    int chunk_num = kv->value.id;
    upgrade_fingerprint_cache_insert_buffer(id, external_file_buffer + 1, chunk_num);
    return 1;
}

int upgrade_external_cache_prefetch_DB(containerid id) {
	upgrade_index_kv_t *kv; // sql insertion buffer
	size_t valueSize;
	int ret = getDB(DB_UPGRADE, &id, sizeof(containerid), &kv, &valueSize);
	if (ret) {
		DEBUG("upgrade_external_cache_prefetch: The index container %lld has not been written!", id);
		return 0;
	}
	if (valueSize % sizeof(upgrade_index_kv_t) != 0 || valueSize == 0) {
		WARNING("Error! valueSize = %d", valueSize);
		exit(1);
	}
	upgrade_fingerprint_cache_insert_buffer(id, kv, valueSize / sizeof(upgrade_index_kv_t));
	free(kv);
	return 1;
}

int upgrade_external_cache_prefetch_htb(containerid id) {
    assert(0); // 不用删除
    if (lru_hashmap_lookup(external_cache_htb, &id)) {
        // 将external cache命中的数据(第一个)放入in-memory cache

        // 从external cache中删除
        struct lruCache *lru = external_cache_htb->lru;
        GList* elem = g_list_first(lru->elem_queue);
        if (lru->size == 1) {
            assert(elem == lru->elem_queue_tail);
            lru->elem_queue_tail = NULL;
        }
        lru->elem_queue = g_list_remove_link(lru->elem_queue, elem);

        void **victim = (void **)elem->data;
        g_list_free_1(elem);
        lru->size--;
        assert(g_hash_table_remove(external_cache_htb->map, victim[0]));

        // 加入in-memory cache
        void *key = NULL, *value = NULL;
        // lru_hashmap_insert_and_retrive(upgrade_cache, victim[0], victim[1], &key, &value);
        free(victim);
        assert((key && value) || (!key && !value));
        if (key) {
            lru_hashmap_insert(external_cache_htb, key, value);
        }
        return 1;
    }
    return 0;
}

/**
 * insert into external cache
 */

void upgrade_external_cache_insert_htb(containerid id, GHashTable *htb) {
    void *key = malloc(sizeof(containerid));
    *(containerid *)key = id;
    lru_hashmap_insert(external_cache_htb, key, htb);
}

void upgrade_external_cache_insert_DB(containerid id, GHashTable *htb) {
    GHashTableIter iter;
    gpointer k, v;
    upgrade_index_kv_t *kv = malloc(sizeof(upgrade_index_kv_t) * g_hash_table_size(htb));
    g_hash_table_iter_init(&iter, htb);
    int i = 0;
    while (g_hash_table_iter_next(&iter, &k, &v)) {
        upgrade_index_kv_t *kv_i = kv + i;
        memcpy(&kv_i->old_fp, k, sizeof(fingerprint));
        memcpy(&kv_i->value, v, sizeof(upgrade_index_value_t));
        i++;
    }
    setDB(DB_UPGRADE, &id, sizeof(containerid), kv, sizeof(upgrade_index_kv_t) * g_hash_table_size(htb));
    free(kv);
}

void upgrade_external_cache_insert_file(containerid id, GHashTable *htb) {
    assert(g_hash_table_size(htb) <= MAX_CHUNK_PER_CONTAINER - 1);
    upgrade_index_kv_t *kv = external_file_buffer;
    kv->value.id = g_hash_table_size(htb);
    memcpy(&kv->old_fp, &id, sizeof(containerid));

    GHashTableIter iter;
    gpointer k, v;
    g_hash_table_iter_init(&iter, htb);
    int i = 1;
    while (g_hash_table_iter_next(&iter, &k, &v)) {
        upgrade_index_kv_t *kv_i = kv + i;
        memcpy(&kv_i->old_fp, k, sizeof(fingerprint));
        memcpy(&kv_i->value, v, sizeof(upgrade_index_value_t));
        i++;
    }
    fseek(external_cache_file, id * sizeof(upgrade_index_kv_t) * MAX_CHUNK_PER_CONTAINER, SEEK_SET);
    fwrite(kv, sizeof(upgrade_index_kv_t), MAX_CHUNK_PER_CONTAINER, external_cache_file);
}
