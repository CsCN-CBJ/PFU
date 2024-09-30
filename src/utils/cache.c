#include "cache.h"

DynamicArray *dynamic_array_new() {
    DynamicArray *array = (DynamicArray *)malloc(sizeof(DynamicArray));
    array->capacity = 8;
    array->size = 0;
    array->data = (void **)malloc(sizeof(void *) * array->capacity);
    return array;
}

void dynamic_array_free(DynamicArray *array) {
    free(array->data);
    free(array);
}

void dynamic_array_add(DynamicArray *array, void *element) {
    if (array->size >= array->capacity) {
        array->capacity *= 2;
        array->data = (void **)realloc(array->data, sizeof(void *) * array->capacity);
    }
    array->data[array->size++] = element;
}
