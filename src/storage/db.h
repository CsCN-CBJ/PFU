#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <hiredis/hiredis.h>

void init_ror();
void initDB();
void close_ror();
void closeDB();
void setDB(char *key, size_t keySize, char *value, size_t valueSize);
int getDB(char *key, size_t keySize, char **value, size_t *valueSize);
