/*
 * jcr.h
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#ifndef Jcr_H_
#define Jcr_H_

#include "destor.h"
#include "recipe/recipestore.h"

#define JCR_STATUS_INIT 1
#define JCR_STATUS_RUNNING 2
#define JCR_STATUS_DONE 3

/* job control record */
struct jcr{
	int32_t id;
	int32_t new_id;
	/*
	 * The path of backup or restore.
	 */
	sds path;

    int status;

	int32_t file_num;
	int32_t pre_process_file_num;
	int64_t origin_data_size;
	int64_t data_size;
	int64_t unique_data_size;
	int64_t chunk_num;
	int32_t unique_chunk_num;
	int32_t zero_chunk_num;
	int64_t zero_chunk_size;
	int32_t rewritten_chunk_num;
	int64_t rewritten_chunk_size;

	int32_t sparse_container_num;
	int32_t inherited_sparse_num;
	int32_t total_container_num;

	uint32_t hash_num;

	struct backupVersion* bv;
	struct backupVersion* new_bv;

	double total_time;
	/*
	 * the time consuming of six dedup phase
	 */
	double read_time;
	double chunk_time;
	double hash_time;
	double pre_dedup_time;
	double dedup_time;
	double rewrite_time;
	double filter_time;
	double write_time;

	double read_recipe_time;
	double read_chunk_time;
	double write_chunk_time;
	double pre_process_container_time;
	double recipe_time;
	double container_filter_time;
	double pre_process_recipe_time;
	double memory_cache_time;
	double external_cache_time;
	uint32_t processed_container_num;
	uint32_t container_processed;
	double file_start_time;
	double file_end_time;
	double container_start_time;
	double container_end_time;
	double in_file_time;
	double in_container_time;

	uint32_t sql_insert;
	uint32_t sql_insert_all;
	uint32_t sql_fetch;
	uint32_t sql_fetch_buffered;

	uint32_t read_container_num;
	uint32_t read_container_new;
	uint32_t read_container_new_buffered; 
	uint32_t sync_buffer_num;
	uint32_t logic_recipe_unique_container;
	uint32_t physical_recipe_unique_container;

	uint32_t recipe_hit;
};

extern struct jcr jcr;

void init_jcr(char *path);
void init_backup_jcr(char *path);
void init_restore_jcr(int revision, char *path);
void init_update_jcr(int revision, char *path);
void print_jcr_result(FILE *fp);

#endif /* Jcr_H_ */
