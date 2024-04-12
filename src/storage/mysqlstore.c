#include "mysqlstore.h"
#include "../jcr.h"
#include "../destor.h"

MYSQL *conn;
MYSQL_STMT *stmt_insert, *stmt_select, *stmt_insert_20;

void init_sql() {
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

    stmt_insert = mysql_stmt_init(conn);
    if (mysql_stmt_prepare(stmt_insert, INSERT_QUERY, strlen(INSERT_QUERY))) {
        fprintf(stderr, "\n mysql_stmt_prepare(), INSERT failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_insert));
        exit(0);
    }

    stmt_insert_20 = mysql_stmt_init(conn);
    if (mysql_stmt_prepare(stmt_insert_20, INSERT_QUERY_20, strlen(INSERT_QUERY_20))) {
        fprintf(stderr, "\n mysql_stmt_prepare(), INSERT failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_insert_20));
        exit(0);
    }

    stmt_select = mysql_stmt_init(conn);
    if (mysql_stmt_prepare(stmt_select, SELECT_QUERY, strlen(SELECT_QUERY))) {
        fprintf(stderr, "\n mysql_stmt_prepare(), INSERT failed");
        fprintf(stderr, "\n %s", mysql_stmt_error(stmt_select));
        exit(0);
    }
}

static void bind_and_execute(MYSQL_STMT *stmt, MYSQL_BIND *bind) {
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
}

void insert_sql_20(char *key, int keySize, char *value, int valueSize) {
    MYSQL_BIND *bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * 20 * 2);
    memset(bind, 0, sizeof(MYSQL_BIND) * 20 * 2);

    for (int i = 0; i < 20; i++) {
        bind[i * 2].buffer_type = MYSQL_TYPE_BLOB;
        bind[i * 2].buffer = key;
        bind[i * 2].buffer_length = keySize;
        bind[i * 2].is_null = 0;

        bind[i * 2 + 1].buffer_type = MYSQL_TYPE_BLOB;
        bind[i * 2 + 1].buffer = value;
        bind[i * 2 + 1].buffer_length = valueSize;
        bind[i * 2 + 1].is_null = 0;
    }

    bind_and_execute(stmt_insert_20, bind);
    mysql_stmt_free_result(stmt_insert_20);
    free(bind);
}

#define INSERT_BATCH 20
void insert_sql_multi(char *key, int keySize, char *value, int valueSize, int count) {
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
        insert_sql(key, keySize, value, valueSize);
        key += keySize;
        value += valueSize;
    }
}

/**
 * return 0 if success, 1 if failed
*/
int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize) {
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_BLOB;
    bind[0].buffer = key;
    bind[0].buffer_length = keySize;
    bind[0].is_null = 0;


    bind_and_execute(stmt_select, &bind[0]);

    unsigned long real_length;
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

void close_sql() {
    mysql_close(conn);
}
