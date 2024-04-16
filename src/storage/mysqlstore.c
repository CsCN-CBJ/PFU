#include "mysqlstore.h"
#include "../jcr.h"
#include "../destor.h"

static MYSQL *conn;
pthread_mutex_t mysql_lock;
MYSQL_STMT *stmt_insert, *stmt_select, *stmt_insert_20;
#define MYSQL_STORE_TABLE "test"

void prepare_stmt(MYSQL_STMT **stmt, const char *query) {
    *stmt = mysql_stmt_init(conn);
    if (mysql_stmt_prepare(*stmt, query, strlen(query))) {
        fprintf(stderr, "\n mysql_stmt_prepare(), failed %s", query);
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
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

    prepare_stmt(&stmt_insert, "INSERT INTO " MYSQL_STORE_TABLE " (k, v) VALUES (?, ?)");
    prepare_stmt(&stmt_select, "SELECT v FROM " MYSQL_STORE_TABLE " where k=?");
    prepare_stmt(&stmt_insert_20, "INSERT INTO " MYSQL_STORE_TABLE " (k, v) VALUES "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), "
    "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?)"
    );
}

void bind_and_execute(MYSQL_STMT *stmt, MYSQL_BIND *bind) {
    if (mysql_stmt_bind_param(stmt, bind)) {
        fprintf(stderr, "\n param bind failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
    
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);
    if (mysql_stmt_execute(stmt)) {
        fprintf(stderr, "\n mysql_stmt_execute failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt));
        exit(0);
    }
    TIMER_END(1, jcr.temp_time2);
}

void insert_sql(char *key, int keySize, char *value, int valueSize) {
    pthread_mutex_lock(&mysql_lock);
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
    pthread_mutex_unlock(&mysql_lock);
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
int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize) {
    pthread_mutex_lock(&mysql_lock);
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
    pthread_mutex_unlock(&mysql_lock);
    if (status == MYSQL_NO_DATA) {
        return 1;
    } 
    return 0;
}

void close_sql() {
    mysql_close(conn);
}
