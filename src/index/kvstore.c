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

    switch(destor.index_key_value_store){
    	case INDEX_KEY_VALUE_HTABLE:
    		// init_kvstore_mysql();
			// if (destor.upgrade_level == UPGRADE_1D_RELATION) {
			// 	init_upgrade_kvstore_htable(destor.index_key_size, sizeof(upgrade_index_value_t));
			// } else if (destor.upgrade_level == UPGRADE_2D_RELATION) {
			// 	init_upgrade_kvstore_htable(sizeof(int64_t), sizeof(int64_t));
			// }
			init_kvstore_htable();

    		close_kvstore = close_kvstore_htable;
    		kvstore_lookup = kvstore_htable_lookup;
    		kvstore_update = kvstore_htable_update;
			// kvstore_multi_update = kvstore_mysql_multi_update;
    		kvstore_delete = kvstore_htable_delete;

			// close_upgrade_kvstore = close_upgrade_kvstore_htable;
			// upgrade_kvstore_lookup = upgrade_kvstore_htable_lookup;
			// upgrade_kvstore_update = upgrade_kvstore_htable_update;

    		break;
		// case INDEX_KEY_VALUE_MYSQL:
		// 	init_kvstore_mysql();
		// 	close_kvstore = close_kvstore_mysql;
		// 	kvstore_lookup = kvstore_mysql_lookup;
		// 	kvstore_update = kvstore_mysql_update;
		// 	kvstore_delete = kvstore_mysql_delete;
		// case INDEX_KEY_VALUE_ROR:
		// 	init_kvstore_ror();
		// 	close_kvstore = close_kvstore_ror;
		// 	kvstore_lookup = kvstore_ror_lookup;
		// 	kvstore_update = kvstore_ror_update;
		// 	kvstore_delete = kvstore_ror_delete;
    	// 	break;
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
