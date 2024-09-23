/*
 * cache.c
 *
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "lru_cache.h"

/*
 * The container read cache.
 */
struct lruCache* new_lru_cache(int size, void (*free_elem)(void *),
		int (*hit_elem)(void* elem, void* user_data)) {
	struct lruCache* c = (struct lruCache*) malloc(sizeof(struct lruCache));

	c->elem_queue = NULL;
	c->elem_queue_tail = NULL;

	c->max_size = size;
	c->size = 0;
	c->hit_count = 0;
	c->miss_count = 0;

	c->free_elem = free_elem;
	c->hit_elem = hit_elem;

	return c;
}

void free_lru_cache(struct lruCache* c) {
	g_list_free_full(c->elem_queue, c->free_elem);
	free(c);
}

/* find a item in cache matching the condition */
void* lru_cache_lookup(struct lruCache* c, void* user_data) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (c->hit_elem(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		if (!elem->next && elem->prev) {
			// 是尾部 且不是头部(头部不用动)
			c->elem_queue_tail = elem->prev;
		}
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->elem_queue = g_list_concat(elem, c->elem_queue);
		c->hit_count++;
		return elem->data;
	} else {
		c->miss_count++;
		return NULL;
	}
}

void* lru_cache_lookup_without_update(struct lruCache* c, void* user_data) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (c->hit_elem(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		return elem->data;
	} else {
		return NULL;
	}
}
/*
 * Hit an existing elem for simulating an insertion of it.
 */
void* lru_cache_hits(struct lruCache* c, void* user_data,
		int (*hit)(void* elem, void* user_data)) {
	GList* elem = g_list_first(c->elem_queue);
	while (elem) {
		if (hit(elem->data, user_data))
			break;
		elem = g_list_next(elem);
	}
	if (elem) {
		if (!elem->next && elem->prev) {
			// 是尾部 且不是头部(头部不用动)
			c->elem_queue_tail = elem->prev;
		}
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->elem_queue = g_list_concat(elem, c->elem_queue);
		return elem->data;
	} else {
		return NULL;
	}
}

/*
 * We know that the data does not exist!
 */
void lru_cache_insert(struct lruCache *c, void* data,
		void (*func)(void*, void*), void* user_data) {
	void *victim = 0;
	if (c->max_size > 0 && c->size == c->max_size) {
		GList *last = c->elem_queue_tail;
		c->elem_queue_tail = last->prev;
		c->elem_queue = g_list_remove_link(c->elem_queue, last);
		victim = last->data;
		g_list_free_1(last);
		c->size--;
	}

	c->elem_queue = g_list_prepend(c->elem_queue, data);
	if (!c->elem_queue_tail) {
		// 说明刚刚加进去的是唯一一个元素
		c->elem_queue_tail = c->elem_queue;
	}
	c->size++;
	if (victim) {
		if (func)
			func(victim, user_data);
		c->free_elem(victim);
	}
}

/* kick out the first elem satisfying func */
void lru_cache_kicks(struct lruCache* c, void* user_data,
		int (*func)(void* elem, void* user_data)) {
	GList* elem = g_list_last(c->elem_queue);
	while (elem) {
		if (func(elem->data, user_data))
			break;
		elem = g_list_previous(elem);
	}
	if (elem) {
		c->elem_queue = g_list_remove_link(c->elem_queue, elem);
		c->free_elem(elem->data);
		g_list_free_1(elem);
		c->size--;
	}
}

int lru_cache_is_full(struct lruCache* c) {
	if (c->max_size < 0)
		return 0;
	return c->size >= c->max_size ? 1 : 0;
}

// 不考虑统计变量
lruHashMap_t *new_lru_hashmap(int size, void (*free_value)(void *),
		GHashFunc hash_func, GEqualFunc equal_func) {
	lruHashMap_t *c = (lruHashMap_t *) malloc(sizeof(lruHashMap_t));
	c->lru = new_lru_cache(size, free_value, NULL);
	c->map = g_hash_table_new_full(hash_func, equal_func, free, NULL);
	return c;
}

void free_lru_hashmap(lruHashMap_t *c) {
	// 有点困难
	assert(0);
	free_lru_cache(c->lru);
	g_hash_table_destroy(c->map);
	free(c);
}

void* lru_hashmap_lookup(lruHashMap_t *c, void* key) {
	GList *elem = (GList *)g_hash_table_lookup(c->map, key);
	if (!elem) {
		return NULL;
	}
	if (!elem->next && elem->prev) {
		// 是尾部 且不是头部(头部不用动)
		c->lru->elem_queue_tail = elem->prev;
	}
	c->lru->elem_queue = g_list_remove_link(c->lru->elem_queue, elem);
	c->lru->elem_queue = g_list_concat(elem, c->lru->elem_queue);
	return ((void **)elem->data)[1];
}

void lru_hashmap_insert(lruHashMap_t *c, void* key, void* value) {
	struct lruCache *lru = c->lru;

	if (lru->max_size > 0 && lru->size == lru->max_size) {
		GList *last = lru->elem_queue_tail;
		lru->elem_queue_tail = last->prev;
		lru->elem_queue = g_list_remove_link(lru->elem_queue, last);
		
		void *victim = last->data;
		g_list_free_1(last);
		lru->size--;
		
		assert(g_hash_table_remove(c->map, ((void **)victim)[0]));
		lru->free_elem(((void **)victim)[1]);
		free(victim);
	}

	void **data = (void **)malloc(sizeof(void *) * 2);
	data[0] = key;
	data[1] = value;
	lru->elem_queue = g_list_prepend(lru->elem_queue, data);
	g_hash_table_insert(c->map, key, g_list_first(lru->elem_queue));
	if (!lru->elem_queue_tail) {
		// 说明刚刚加进去的是唯一一个元素
		lru->elem_queue_tail = lru->elem_queue;
	}
	lru->size++;
}
