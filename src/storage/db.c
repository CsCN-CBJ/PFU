#include "../jcr.h"
#include "db.h"

#define REDIS_ADDR "127.0.0.1"
#define REDIS_PORT 6666

static redisContext *connList[DB_ALL];
static pthread_mutex_t dbLock[DB_ALL];

void init_ror(int index) {
    struct timeval timeout = { 5, 0 };
    connList[index] = redisConnectWithTimeout(REDIS_ADDR, REDIS_PORT + index, timeout);
    redisContext *conn = connList[index];
    if (conn == NULL || conn->err) {
        if (conn) {
            fprintf(stderr, "Index %d connection error: %d %s\n", index, conn->err, conn->errstr);
            redisFree(conn);
        } else {
            perror("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }
    redisReply *reply = redisCommand(conn, "FLUSHALL", index);
    if (reply == NULL) {
        fprintf(stderr, "redisCommand failed: %s\n", conn->errstr);
        exit(1);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "redisCommand returned error: %s\n", conn->errstr);
        exit(1);
    }
    WARNING("redis connection %d success", index);
    freeReplyObject(reply);
}

void initDB(int index) {
    pthread_mutex_init(&dbLock[index], NULL);
    init_ror(index);
}

void close_ror(int index) {
    redisFree(connList[index]);
}

void closeDB(int index) {
    close_ror(index);
    pthread_mutex_destroy(&dbLock[index]);
}

void setDB(int index, char *key, size_t keySize, char *value, size_t valueSize) {
    pthread_mutex_lock(&dbLock[index]);
    jcr.sql_insert++;
    redisReply *reply = redisCommand(connList[index], "SET %b %b", key, keySize, value, valueSize);
    freeReplyObject(reply);
    pthread_mutex_unlock(&dbLock[index]);
}

int getDB(int index, char *key, size_t keySize, char **value, size_t *valueSize) {
    int res;
    pthread_mutex_lock(&dbLock[index]);
    jcr.sql_fetch++;
    redisReply *reply = redisCommand(connList[index], "GET %b", key, keySize);
    *value = NULL;
    *valueSize = 0;
    if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "redisCommand returned error: %s %d\n", connList[index]->errstr, connList[index]->err);
        exit(6);
    } else if (reply->type == REDIS_REPLY_NIL) {
        res = -1;
    } else {
        char *valueCopy = malloc(reply->len);
        memcpy(valueCopy, reply->str, reply->len);
        *value = valueCopy;
        *valueSize = reply->len;
        res = 0;
    }
    freeReplyObject(reply);
    pthread_mutex_unlock(&dbLock[index]);
    return res;
}
