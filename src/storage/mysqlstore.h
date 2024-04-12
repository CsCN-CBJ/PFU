#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mysql/mysql.h>

#define SERVER "localhost"
#define USER "root"
#define PASSWORD "root"
#define DATABASE "CBJ"
#define TABLE "test"
#define INSERT_QUERY "INSERT INTO " TABLE " (k, v) VALUES (?, ?)"
#define INSERT_QUERY_20 "INSERT INTO " TABLE " (k, v) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)"
#define SELECT_QUERY "SELECT v FROM " TABLE " where k=?"

void init_sql();
void insert_sql(char *key, int keySize, char *value, int valueSize);
void insert_sql_multi(char *key, int keySize, char *value, int valueSize, int count);
int fetch_sql(char *key, int keySize, char *value, int valueBufferLen, unsigned long *valueSize);
void close_sql();
