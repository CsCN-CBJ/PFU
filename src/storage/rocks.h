#ifndef ROCKS_H_
#define ROCKS_H_

#include "db.h"

void init_RocksDB(int index);
void close_RocksDB(int index);
void put_RocksDB(int index, char *key, size_t keySize, char *value, size_t valueSize);
void get_RocksDB(int index, char *key, size_t keySize, char **value, size_t *valueSize);

#endif /* ROCKS_H_ */
