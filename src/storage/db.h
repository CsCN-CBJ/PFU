#ifndef DB_H_
#define DB_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

enum {
    DB_UPGRADE,
    DB_KVSTORE,
    DB_ALL,
};

// void initDB(int index);
// void closeDB(int index);
// void setDB(int index, char *key, size_t keySize, char *value, size_t valueSize);
// int getDB(int index, char *key, size_t keySize, char **value, size_t *valueSize);

#endif /* DB_H_ */
