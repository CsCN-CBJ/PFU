/*
 * update.h
 *
 *  Created on: Mar 10, 2024
 *      Author: Boju Chen
 */

#ifndef UPDATE_H_
#define UPDATE_H_

#include "utils/sync_queue.h"

SyncQueue *update_recipe_queue;
SyncQueue *update_chunk_queue;

#endif /* UPDATE_H_ */
