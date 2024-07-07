#include "mysqlstore.h"
#include "../jcr.h"
#include "../destor.h"
#include "../index/fingerprint_cache.h"
#include "../index/index.h"

static MYSQL *conn;
pthread_mutex_t mysql_lock;
char *stmt_buf;
MYSQL_STMT *stmt_insert, *stmt_select, *stmt_insert_20, *stmt_insert_buffer;
#define MYSQL_STORE_TABLE "test"

#define SQL_BUFFER_LEN 600
MYSQL_BIND *bind_buffer = NULL;
static int bind_buffer_cnt = 0;
GHashTable *sql_buffer_htb;

void prepare_stmt(MYSQL_STMT **stmt, const char *query) {
    *stmt = mysql_stmt_init(conn);
    if (mysql_stmt_prepare(*stmt, query, strlen(query))) {
        fprintf(stderr, "\n mysql_stmt_prepare(), failed %s", query);
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
}

void generate_insert_stmt(char *buf, char *baseStr, int count) {
    strcpy(buf, baseStr);
    for (int i = 0; i < count; i++) {
        strcat(buf, "(?, ?), ");
    }
    buf[strlen(buf) - 2] = '\0';
}

void init_sql() {
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

    stmt_buf = (char *)malloc(10240);
    prepare_stmt(&stmt_insert, "INSERT INTO " MYSQL_STORE_TABLE " (k, v) VALUES (?, ?)");
    prepare_stmt(&stmt_select, "SELECT v FROM " MYSQL_STORE_TABLE " where k=?");
    generate_insert_stmt(stmt_buf, "INSERT INTO " MYSQL_STORE_TABLE " (k, v) VALUES ", 20);
    prepare_stmt(&stmt_insert_20, stmt_buf);
    generate_insert_stmt(stmt_buf, "INSERT INTO " MYSQL_STORE_TABLE " (k, v) VALUES ", SQL_BUFFER_LEN);
    prepare_stmt(&stmt_insert_buffer, stmt_buf);

    bind_buffer = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * SQL_BUFFER_LEN * 2);
    sql_buffer_htb = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
}

void bind_and_execute(MYSQL_STMT *stmt, MYSQL_BIND *bind) {
    if (mysql_stmt_bind_param(stmt, bind)) {
        fprintf(stderr, "\n param bind failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
    
    if (mysql_stmt_execute(stmt)) {
        fprintf(stderr, "\n mysql_stmt_execute failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
}

void insert_sql(char *key, int keySize, char *value, int valueSize) {
    pthread_mutex_lock(&mysql_lock);
    jcr.sql_insert++;
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_BLOB;
    bind[0].buffer = key;
    bind[0].buffer_length = keySize;
    // bind[0].is_null = 0;

    bind[1].buffer_type = MYSQL_TYPE_BLOB;
    bind[1].buffer = value;
    bind[1].buffer_length = valueSize;
    // bind[1].is_null = 0;

    bind_and_execute(stmt_insert, bind);
    mysql_stmt_free_result(stmt_insert);
    pthread_mutex_unlock(&mysql_lock);
}

void insert_sql_store_buffered_1D(fingerprint *fp, upgrade_index_value_t *value) {
    pthread_mutex_lock(&mysql_lock);
    jcr.sql_insert_all++;

    upgrade_index_kv_t *kv = (upgrade_index_kv_t *)malloc(sizeof(upgrade_index_kv_t));
    memcpy(&kv->old_fp, fp, sizeof(fingerprint));
    memcpy(&kv->value, value, sizeof(upgrade_index_value_t));
    g_hash_table_insert(sql_buffer_htb, &kv->old_fp, &kv->value);

    MYSQL_BIND *bind = bind_buffer + bind_buffer_cnt * 2;
    bind->buffer_type = MYSQL_TYPE_BLOB;
    bind->buffer = &kv->old_fp;
    bind->buffer_length = sizeof(fingerprint);
    bind->is_null = 0;

    bind++;
    bind->buffer_type = MYSQL_TYPE_BLOB;
    bind->buffer = &kv->value;
    bind->buffer_length = sizeof(upgrade_index_value_t);
    bind->is_null = 0;

    bind_buffer_cnt++;

    if (bind_buffer_cnt >= SQL_BUFFER_LEN) {
        bind_and_execute(stmt_insert_buffer, bind_buffer);
        bind_buffer_cnt = 0;
        g_hash_table_destroy(sql_buffer_htb);
        memset(bind_buffer, 0, sizeof(MYSQL_BIND) * SQL_BUFFER_LEN * 2);
        sql_buffer_htb = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
        jcr.sql_insert++;
    }
    pthread_mutex_unlock(&mysql_lock);
}

int fetch_sql_buffered_1D(fingerprint *fp, upgrade_index_value_t *value) {
    pthread_mutex_lock(&mysql_lock);
    
    upgrade_index_value_t *v = g_hash_table_lookup(sql_buffer_htb, fp);
    
    int ret;
    if (v) {
        jcr.sql_fetch_buffered++;
        ret = 0;
        memcpy(value, v, sizeof(upgrade_index_value_t));
    } else {
        jcr.sql_fetch++;
        unsigned long valueSize;
        ret = _fetch_sql((char *)fp, sizeof(fingerprint), (char *)value, sizeof(upgrade_index_value_t), &valueSize);
        assert(ret == 1 || (ret == 0 && valueSize == sizeof(upgrade_index_value_t)));
    }

    pthread_mutex_unlock(&mysql_lock);
    return ret;
}

#define INSERT_BATCH 20
void insert_sql_multi(char *key, int keySize, char *value, int valueSize, int count) {
    pthread_mutex_lock(&mysql_lock);
    MYSQL_BIND *bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * INSERT_BATCH * 2);
    memset(bind, 0, sizeof(MYSQL_BIND) * INSERT_BATCH * 2);

    for (int batch = 0; batch < count / INSERT_BATCH; batch++) {
        for (int i = 0; i < INSERT_BATCH; i++) {
            bind[i * 2].buffer_type = MYSQL_TYPE_BLOB;
            bind[i * 2].buffer = key;
            bind[i * 2].buffer_length = keySize;
            bind[i * 2].is_null = 0;

            bind[i * 2 + 1].buffer_type = MYSQL_TYPE_BLOB;
            bind[i * 2 + 1].buffer = value;
            bind[i * 2 + 1].buffer_length = valueSize;
            bind[i * 2 + 1].is_null = 0;

            key += keySize;
            value += valueSize;
        }
        bind_and_execute(stmt_insert_20, bind);
        mysql_stmt_free_result(stmt_insert_20);
    }
    free(bind);

    for (int i = 0; i < count % INSERT_BATCH; i++) {
        MYSQL_BIND bind[2];
        memset(bind, 0, sizeof(bind));

        bind[0].buffer_type = MYSQL_TYPE_BLOB;
        bind[0].buffer = key;
        bind[0].buffer_length = keySize;
        bind[0].is_null = 0;

        bind[1].buffer_type = MYSQL_TYPE_BLOB;
        bind[1].buffer = value;
        bind[1].buffer_length = valueSize;
        bind[1].is_null = 0;

        bind_and_execute(stmt_insert, bind);
        mysql_stmt_free_result(stmt_insert);
        key += keySize;
        value += valueSize;
    }
    pthread_mutex_unlock(&mysql_lock);
}

/**
 * return 0 if success, 1 if failed
*/
int _fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize) {
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_BLOB;
    bind[0].buffer = key;
    bind[0].buffer_length = keySize;
    bind[0].is_null = 0;

    bind_and_execute(stmt_select, &bind[0]);

    my_bool is_null;
    bind[1].buffer_type = MYSQL_TYPE_BLOB;
    bind[1].buffer = value;
    bind[1].buffer_length = valueBufferLen;
    bind[1].length = valueSize;
    bind[1].is_null = &is_null;
    if (mysql_stmt_bind_result(stmt_select, &bind[1])) {
        fprintf(stderr, "mysql_stmt_bind_result failed\n");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_select));
    }

    // 插入二进制数据
    int status = mysql_stmt_fetch(stmt_select);
    if (status == 1) {
        fprintf(stderr, "mysql_stmt_fetch failed\n");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_select));
        exit(0);
    }
    mysql_stmt_free_result(stmt_select);
    if (status == MYSQL_NO_DATA) {
        return 1;
    } 
    return 0;
}

int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize) {
    pthread_mutex_lock(&mysql_lock);
    jcr.sql_fetch++;
    int ret = _fetch_sql(key, keySize, value, valueBufferLen, valueSize);
    pthread_mutex_unlock(&mysql_lock);
    return ret;
}

void close_sql() {
    // 写入最后一批数据
    MYSQL_BIND *bind = bind_buffer;
    for (int i = 0; i < bind_buffer_cnt / INSERT_BATCH; i++) {
        bind_and_execute(stmt_insert_20, bind);
        mysql_stmt_free_result(stmt_insert_20);
        bind += INSERT_BATCH * 2;
    }
    for (int i = 0; i < bind_buffer_cnt % INSERT_BATCH; i++) {
        bind_and_execute(stmt_insert, bind);
        mysql_stmt_free_result(stmt_insert);
        bind += 2;
    }
    assert(bind == bind_buffer + bind_buffer_cnt * 2);
    mysql_close(conn);
    // BUGS: buffer如果非空, 最后不会往sql里面写入
    free(bind_buffer);
    g_hash_table_destroy(sql_buffer_htb);
}
