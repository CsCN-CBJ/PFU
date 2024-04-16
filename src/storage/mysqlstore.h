#ifndef MYSQLSTORE_H_
#define MYSQLSTORE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mysql/mysql.h>

#define SERVER "localhost"
#define USER "root"
#define PASSWORD "root"
#define DATABASE "CBJ"

void prepare_stmt(MYSQL_STMT **stmt, const char *query);
void bind_and_execute(MYSQL_STMT *stmt, MYSQL_BIND *bind);
void init_sql();
void insert_sql(char *key, int keySize, char *value, int valueSize);
void insert_sql_multi(char *key, int keySize, char *value, int valueSize, int count);
int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize);
void close_sql();

#endif /* MYSQLSTORE_H_ */
