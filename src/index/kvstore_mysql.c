#include "../destor.h"
#include "../storage/mysqlstore.h"
#include <mysql/mysql.h>

#define KVSTORE_TABLE "kvstore"
#define KVSTORE_CLEAR_TABLE "TRUNCATE TABLE " KVSTORE_TABLE
static MYSQL *conn;
MYSQL_STMT *stmt_kvstore_insert, *stmt_kvstore_select, *stmt_kvstore_insert_20;
pthread_mutex_t mysql_lock;

void init_kvstore_mysql() {
    pthread_mutex_init(&mysql_lock, NULL);
    conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "mysql_init() failed\n");
        exit(1);
    }

    if (mysql_real_connect(conn, SERVER, USER, PASSWORD, DATABASE, 0, NULL, 0) == NULL) {
        fprintf(stderr, "mysql_real_connect() failed\n");
        fprintf(stderr, "%s\n", mysql_error(conn));
        mysql_close(conn);
        exit(1);    
    }
    
    if (job == DESTOR_UPDATE) {
        mysql_real_query(conn, KVSTORE_CLEAR_TABLE, strlen(KVSTORE_CLEAR_TABLE));
    }
    prepare_stmt(&stmt_kvstore_insert, "INSERT INTO " KVSTORE_TABLE " (k, v) VALUES (?, ?)");
    prepare_stmt(&stmt_kvstore_select, "SELECT v FROM " KVSTORE_TABLE " where k=?");
    prepare_stmt(&stmt_kvstore_insert_20, "INSERT INTO " KVSTORE_TABLE " (k, v) VALUES "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?)"
    );
}

void close_kvstore_mysql() {
    pthread_mutex_destroy(&mysql_lock);
    return;
}

/**
 * should free the return value after use
*/
int64_t* kvstore_mysql_lookup(char *key) {
    pthread_mutex_lock(&mysql_lock);
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_BLOB;
    bind[0].buffer = key;
    bind[0].buffer_length = destor.index_key_size;
    bind[0].is_null = 0;

    bind_and_execute(stmt_kvstore_select, &bind[0]);

    int64_t *value = (int64_t*)malloc(sizeof(int64_t));
    unsigned long real_length;
    my_bool is_null;
    bind[1].buffer_type = MYSQL_TYPE_BLOB;
    bind[1].buffer = value;
    bind[1].buffer_length = sizeof(int64_t);
    bind[1].length = &real_length;
    bind[1].is_null = &is_null;
    if (mysql_stmt_bind_result(stmt_kvstore_select, &bind[1])) {
        fprintf(stderr, "mysql_stmt_bind_result failed\n");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_kvstore_select));
    }

    int status = mysql_stmt_fetch(stmt_kvstore_select);
    if (status == 1) {
        fprintf(stderr, "mysql_stmt_fetch failed\n");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_kvstore_select));
        exit(0);
    }
    mysql_stmt_free_result(stmt_kvstore_select);
    pthread_mutex_unlock(&mysql_lock);
    if (status == MYSQL_NO_DATA) {
        free(value);
        return NULL;
    }
    assert(real_length == sizeof(int64_t));
    return value;
}

void kvstore_mysql_update(char *key, int64_t id) {
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_BLOB;
    bind[0].buffer = key;
    bind[0].buffer_length = destor.index_key_size;
    bind[0].is_null = 0;

    bind[1].buffer_type = MYSQL_TYPE_BLOB;
    bind[1].buffer = &id;
    bind[1].buffer_length = sizeof(int64_t);
    bind[1].is_null = 0;

    bind_and_execute(stmt_kvstore_insert, bind);
    mysql_stmt_free_result(stmt_kvstore_insert);
}

#define INSERT_BATCH 20
void kvstore_mysql_multi_update(GHashTable *htb, int64_t id) {
    pthread_mutex_lock(&mysql_lock);
    MYSQL_BIND *bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * INSERT_BATCH * 2);
    memset(bind, 0, sizeof(MYSQL_BIND) * INSERT_BATCH * 2);
    int length = g_hash_table_size(htb);
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, htb);

    int keySize = destor.index_key_size;
    for (int batch = 0; batch < length / INSERT_BATCH; batch++) {
        for (int i = 0; i < INSERT_BATCH; i++) {

            assert(g_hash_table_iter_next(&iter, &key, &value));

            bind[i * 2].buffer_type = MYSQL_TYPE_BLOB;
            bind[i * 2].buffer = key;
            bind[i * 2].buffer_length = keySize;
            bind[i * 2].is_null = 0;

            bind[i * 2 + 1].buffer_type = MYSQL_TYPE_BLOB;
            bind[i * 2 + 1].buffer = &id;
            bind[i * 2 + 1].buffer_length = sizeof(int64_t);
            bind[i * 2 + 1].is_null = 0;
        }
        bind_and_execute(stmt_kvstore_insert_20, bind);
        mysql_stmt_free_result(stmt_kvstore_insert_20);
    }
    free(bind);

    for (int i = 0; i < length % INSERT_BATCH; i++) {
        assert(g_hash_table_iter_next(&iter, &key, &value));
        kvstore_mysql_update(key, id);
    }
    pthread_mutex_unlock(&mysql_lock);
}

void kvstore_mysql_delete(char* key, int64_t id) {
    // useless function
    assert(0);
}

