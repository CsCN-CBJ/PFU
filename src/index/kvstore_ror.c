#include "../storage/db.h"
#include "../destor.h"
#include <assert.h>

void init_kvstore_ror();
void close_kvstore_ror();
int64_t* kvstore_ror_lookup(char* key);
void kvstore_ror_update(char* key, int64_t id);
void kvstore_ror_delete(char* key, int64_t id);

void init_kvstore_ror() {
    initDB(DB_KVSTORE);
}

void close_kvstore_ror() {
    closeDB(DB_KVSTORE);
}

void kvstore_ror_delete(char* key, int64_t id) {
    // not implemented
    assert(0);
}

int64_t* kvstore_ror_lookup(char* key) {
    assert(destor.index_key_size == 32);
    char *value;
    size_t valueSize, res;
    res = getDB(DB_KVSTORE, key, destor.index_key_size, &value, &valueSize);
    if (res) return NULL;

    assert(valueSize == sizeof(int64_t));
    return (int64_t*) value;
}

void kvstore_ror_update(char* key, int64_t id) {
    setDB(DB_KVSTORE, key, destor.index_key_size, (char *)&id, sizeof(int64_t));
}
