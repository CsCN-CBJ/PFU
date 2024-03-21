/*
 * fingerprint_cache.h
 *
 *  Created on: Mar 24, 2014
 *      Author: fumin
 */

#ifndef FINGERPRINT_CACHE_H_
#define FINGERPRINT_CACHE_H_

void init_fingerprint_cache();
int64_t fingerprint_cache_lookup(fingerprint *fp);
void fingerprint_cache_prefetch(int64_t id);

typedef struct upgrade_index_value {
    int64_t id;
    fingerprint fp;
} upgrade_index_value_t;

typedef struct upgrade_index_kv {
    fingerprint old_fp;
    struct upgrade_index_value value;
} upgrade_index_kv_t;

void init_upgrade_fingerprint_cache();
upgrade_index_kv_t* upgrade_fingerprint_cache_lookup(fingerprint *old_fp);
void upgrade_fingerprint_cache_insert(fingerprint *old_fp, upgrade_index_value_t *v);

void upgrade_fingerprint_cache_prefetch(int64_t id);
#endif /* FINGERPRINT_CACHE_H_ */
