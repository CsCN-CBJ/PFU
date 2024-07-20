#include "../jcr.h"
#include "db.h"

#define REDIS_ADDR "127.0.0.1"
#define REDIS_PORT 6666

static redisContext *conn;
static pthread_mutex_t dbLock;

void init_ror() {
    struct timeval timeout = { 5, 0 };
    conn = redisConnectWithTimeout(REDIS_ADDR, REDIS_PORT, timeout);
    pthread_mutex_init(&dbLock, NULL);
    if (conn == NULL || conn->err) {
        if (conn) {
            printf("Connection error: %s\n", conn->errstr);
            redisFree(conn);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }
    // 发送FLUSHALL命令清空Redis
    redisReply *reply = (redisReply*)redisCommand(conn, "FLUSHALL");
    if (reply == NULL) {
        printf("FLUSHALL command failed: %s\n", conn->errstr);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        printf("FLUSHALL command returned error: %s\n", reply->str);
    } else {
        printf("Redis has been flushed successfully.\n");
    }
    freeReplyObject(reply);
}

void initDB() {
    init_ror();
}

void close_ror() {
    redisFree(conn);
    pthread_mutex_destroy(&dbLock);
}

void closeDB() {
    close_ror();
}

void setDB(char *key, size_t keySize, char *value, size_t valueSize) {
    pthread_mutex_lock(&dbLock);
    jcr.sql_insert++;
    redisReply *reply = (redisReply*)redisCommand(conn, "SET %b %b", key, keySize, value, valueSize);
    freeReplyObject(reply);
    pthread_mutex_unlock(&dbLock);
}

int getDB(char *key, size_t keySize, char **value, size_t *valueSize) {
    int res;
    pthread_mutex_lock(&dbLock);
    jcr.sql_fetch++;
    redisReply *reply = (redisReply*)redisCommand(conn, "GET %b", key, keySize);
    *value = NULL;
    *valueSize = 0;
    if (reply->type == REDIS_REPLY_NIL) {
        res = -1;
    } else {
        char *valueCopy = malloc(reply->len);
        memcpy(valueCopy, reply->str, reply->len);
        *value = valueCopy;
        *valueSize = reply->len;
        res = 0;
    }
    freeReplyObject(reply);
    pthread_mutex_unlock(&dbLock);
    return res;
}


