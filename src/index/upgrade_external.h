/**
 * @file upgrade_external.h
 * @brief Upgrade external cache
 * Created on: 2024-11-24
 * Author: Boju Chen
 */

#ifndef UPGRADE_EXTERNAL_H
#define UPGRADE_EXTERNAL_H

#include "upgrade_cache.h"

void init_upgrade_external_cache();
void close_upgrade_external_cache();
extern void (*upgrade_external_cache_insert)(containerid id, GHashTable *htb);
extern int (*upgrade_external_cache_prefetch)(containerid id);
int upgrade_external_cache_prefetch_rockfile(containerid id, fingerprint *fp);

#endif /* UPGRADE_EXTERNAL_H */
