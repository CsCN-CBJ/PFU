#include "kvstore.h"

extern void init_kvstore_htable();
extern void close_kvstore_htable();
extern int64_t* kvstore_htable_lookup(char* key);
extern void kvstore_htable_update(char* key, int64_t id);
extern void kvstore_htable_delete(char* key, int64_t id);

// extern void init_kvstore_mysql();
// extern void close_kvstore_mysql();
// extern int64_t* kvstore_mysql_lookup(char* key);
// extern void kvstore_mysql_update(char* key, int64_t id);
// extern void kvstore_mysql_delete(char* key, int64_t id);
// void kvstore_mysql_multi_update(char *key, int64_t *value, int count);

extern void init_upgrade_kvstore_htable(int32_t key_size, int32_t value_size);
extern void close_upgrade_kvstore_htable();
extern void* upgrade_kvstore_htable_lookup(char* key);
extern void upgrade_kvstore_htable_update(char* key, void* value);

// extern void init_kvstore_ror();
// extern void close_kvstore_ror();
// extern int64_t* kvstore_ror_lookup(char* key);
// extern void kvstore_ror_update(char* key, int64_t id);
// extern void kvstore_ror_delete(char* key, int64_t id);

FILE *kvstore_file;
void init_kvstore_file() {
	sds path = sdsdup(destor.working_directory);
	path = sdscat(path, "/kvstore_file");
	kvstore_file = fopen(path, "w");
	sdsfree(path);
}

void close_kvstore_file() {
	fclose(kvstore_file);
}

void kvstore_file_update(char* key, int64_t id) {
	fwrite(key, 1, destor.index_key_size, kvstore_file);
	fwrite(&id, 1, sizeof(int64_t), kvstore_file);
}

void kvstore_file_lookup(char* key) {assert(0);}
void kvstore_file_delete(char* key, int64_t id) {assert(0);}

/*
 * Mapping a fingerprint (or feature) to the prefetching unit.
 */

void (*close_kvstore)();
int64_t* (*kvstore_lookup)(char *key);
void (*kvstore_update)(char *key, int64_t id);
void (*kvstore_multi_update)(GHashTable *htb, int64_t id);
void (*kvstore_delete)(char* key, int64_t id);

void (*close_upgrade_kvstore)();
void* (*upgrade_kvstore_lookup)(char *key);
void (*upgrade_kvstore_update)(char *key, void* value);

void init_kvstore() {

	if (job == DESTOR_BACKUP) destor.index_key_value_store = INDEX_KEY_VALUE_HTABLE;

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
			init_kvstore_htable();

    		close_kvstore = close_kvstore_htable;
    		kvstore_lookup = kvstore_htable_lookup;
    		kvstore_update = kvstore_htable_update;
    		kvstore_delete = kvstore_htable_delete;
    		break;
		case INDEX_KEY_VALUE_FILE:
			init_kvstore_file();
			close_kvstore = close_kvstore_file;
			kvstore_lookup = kvstore_file_lookup;
			kvstore_update = kvstore_file_update;
			kvstore_delete = kvstore_file_delete;
			break;
    	default:
    		WARNING("Invalid key-value store!");
    		exit(1);
    }
}
