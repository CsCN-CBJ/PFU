#ifndef MYSQLSTORE_H_
#define MYSQLSTORE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mysql/mysql.h>
#include "../destor.h"
#include "../index/fingerprint_cache.h"
#include "../index/upgrade_cache.h"

#define SERVER "localhost"
#define USER "root"
#define PASSWORD "root"
#define DATABASE "CBJ"

void prepare_stmt(MYSQL_STMT **stmt, const char *query);
void bind_and_execute(MYSQL_STMT *stmt, MYSQL_BIND *bind);
void init_sql();
void insert_sql(char *key, int keySize, char *value, int valueSize);
void insert_sql_multi(char *key, int keySize, char *value, int valueSize, int count);
int _fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize);
int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize);
void insert_sql_store_buffered_1D(fingerprint *fp, upgrade_index_value_t *value);
int fetch_sql_buffered_1D(fingerprint *fp, upgrade_index_value_t *value);
void close_sql();

#endif /* MYSQLSTORE_H_ */
