/*
 * fingerprint_cache.c
 *
 *  Created on: Mar 24, 2014
 *      Author: fumin
 */
#include "../destor.h"
#include "index.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "../utils/lru_cache.h"
#include "fingerprint_cache.h"

static struct lruCache* lru_queue;
static struct lruCache* upgrade_lru_queue;

/* defined in index.c */
extern struct index_overhead index_overhead, upgrade_index_overhead;

void init_fingerprint_cache(){
	switch(destor.index_category[1]){
	case INDEX_CATEGORY_PHYSICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);
		break;
	case INDEX_CATEGORY_LOGICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_segment_recipe, lookup_fingerprint_in_segment_recipe);
		break;
	default:
		WARNING("Invalid index category!");
		exit(1);
	}
}

int64_t fingerprint_cache_lookup(fingerprint *fp){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta* cm = lru_cache_lookup(lru_queue, fp);
			if (cm)
				return cm->id;
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			struct segmentRecipe* sr = lru_cache_lookup(lru_queue, fp);
			if(sr){
				struct chunkPointer* cp = g_hash_table_lookup(sr->kvpairs, fp);
				if(cp->id <= TEMPORARY_ID){
					WARNING("expect > TEMPORARY_ID, but being %lld", cp->id);
					assert(cp->id > TEMPORARY_ID);
				}
				return cp->id;
			}
			break;
		}
	}

	return TEMPORARY_ID;
}

void fingerprint_cache_prefetch(int64_t id){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta * cm = retrieve_container_meta_by_id(id);
			index_overhead.read_prefetching_units++;
			if (cm) {
				lru_cache_insert(lru_queue, cm, NULL, NULL);
			} else{
				WARNING("Error! The container %lld has not been written!", id);
				exit(1);
			}
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			if (!lru_cache_hits(lru_queue, &id,
					segment_recipe_check_id)){
				/*
				 * If the segment we need is already in cache,
				 * we do not need to read it.
				 */
				GQueue* segments = prefetch_segments(id,
						destor.index_segment_prefech);
				index_overhead.read_prefetching_units++;
				VERBOSE("Dedup phase: prefetch %d segments into %d cache",
						g_queue_get_length(segments),
						destor.index_cache_size);
				struct segmentRecipe* sr;
				while ((sr = g_queue_pop_tail(segments))) {
					/* From tail to head */
					if (!lru_cache_hits(lru_queue, &sr->id,
							segment_recipe_check_id)) {
						lru_cache_insert(lru_queue, sr, NULL, NULL);
					} else {
						/* Already in cache */
						free_segment_recipe(sr);
					}
				}
				g_queue_free(segments);
			}
			break;
		}
	}
}

/**
 * Upgrade fingerprint cache
 * LRU of GHashTable(old_fp, upgrade_index_value_t)
*/

void free_upgrade_index_value(GHashTable **htb) {
	free(htb);
}

int compare_upgrade_index_value(GHashTable **htb, fingerprint *old_fp) {
	return g_hash_table_lookup(*htb, old_fp) != NULL;
}

void init_upgrade_fingerprint_cache() {
	upgrade_lru_queue = new_lru_cache(destor.index_cache_size,
				free_upgrade_index_value, compare_upgrade_index_value);
}

upgrade_index_value_t* upgrade_fingerprint_cache_lookup(fingerprint *old_fp) {
	GHashTable **htb = lru_cache_lookup(upgrade_lru_queue, old_fp);
	if (htb) {
		upgrade_index_value_t* v = g_hash_table_lookup(*htb, old_fp);
		assert(v);
		return v;
	}
	return NULL;
}

void upgrade_fingerprint_cache_insert(GHashTable *htb) {
	GHashTable **htb_p = (GHashTable **) malloc(sizeof(GHashTable *));
	*htb_p = htb;
	lru_cache_insert(upgrade_lru_queue, htb_p, NULL, NULL);
}

void upgrade_fingerprint_cache_prefetch(int64_t id) {
	GHashTable* c = retrieve_upgrade_index_container_by_id(id);
	upgrade_index_overhead.read_prefetching_units++;
	if (!c) {
		WARNING("Error! The index container %lld has not been written!, %lld", id, c);
		exit(1);
	}
	upgrade_fingerprint_cache_insert(c);
}
