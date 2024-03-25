/*
 * update.h
 *
 *  Created on: Mar 10, 2024
 *      Author: Boju Chen
 */

#ifndef UPDATE_H_
#define UPDATE_H_

#include "utils/sync_queue.h"

SyncQueue *upgrade_recipe_queue;
SyncQueue *upgrade_chunk_queue;
SyncQueue *pre_dedup_queue;

#endif /* UPDATE_H_ */
