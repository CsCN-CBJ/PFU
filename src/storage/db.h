#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <hiredis/hiredis.h>

enum {
    DB_UPGRADE,
    DB_KVSTORE,
    DB_ALL,
};

#define DB_UPGRADE 0
#define DB_KVSTORE 1

void initDB(int index);
void closeDB(int index);
void setDB(int index, char *key, size_t keySize, char *value, size_t valueSize);
int getDB(int index, char *key, size_t keySize, char **value, size_t *valueSize);
