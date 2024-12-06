/*
 * similarity.h
 *
 *  Created on: Oct 24, 2024
 *      Author: Boju Chen
 */

#ifndef SIMILAITY_H_
#define SIMILAITY_H_

#include "destor.h"

#define FEATURE_NUM 4
typedef uint64_t feature;
struct featureList {
	feature feature;
	size_t count;
	size_t max_count;
	containerid *recipeIDList;
};

typedef struct recipeUnit {
	struct fileRecipeMeta *recipe;
	struct chunkPointer *chunks;
    struct chunk *cks;
	int64_t chunk_off;

	containerid sub_id;
	containerid total_num;
	containerid chunk_num;

	struct recipeUnit *next;
} recipeUnit_t;

void* read_similarity_recipe_thread(void *arg);
void* pre_process_recipe_thread(void *arg);

#endif /* SIMILAITY_H_ */
