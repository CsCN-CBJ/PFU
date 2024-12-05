/*
 * cache.h
 *	All kinds of cache
 *  Created on: Sep 29, 2024
 *      Author: Boju Chen
 */

#ifndef CBJ_Cache_H_
#define CBJ_Cache_H_

#include <stdlib.h>

typedef struct {
    void **data;
    size_t capacity;
    size_t size;
} DynamicArray;

DynamicArray *dynamic_array_new();
DynamicArray *dynamic_array_new_size(int size);
void dynamic_array_free(DynamicArray *array);
void dynamic_array_free_special(DynamicArray *array, void (*free_element)(void *));
void dynamic_array_add(DynamicArray *array, void *element);
int dynamic_array_get_length(DynamicArray *array);

#endif /* Cache_H_ */
