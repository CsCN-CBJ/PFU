/*
 * Copyright (c) 2014 Sonam Mandal
 * Copyright (c) 2014 Vasily Tarasov
 * Copyright (c) 2014 Will Buik
 * Copyright (c) 2014 Erez Zadok
 * Copyright (c) 2014 Geoff Kuenning
 * Copyright (c) 2014 Stony Brook University
 * Copyright (c) 2014 Harvey Mudd College
 * Copyright (c) 2014 The Research Foundation of the State University of New York
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <string.h>

#include "../destor.h"
#include "../jcr.h"
#include "../backup.h"

/* Use this macros if libhashfile library is installed on your system */
// #include <libhashfile.h>

/* Use this macros if libhashfile library is NOT installed on your system */
#include "libhashfile.h"

#define MAXLINE	4096

static void print_chunk_hash(uint64_t chunk_count, const uint8_t *hash,
					int hash_size_in_bytes)
{
	int j;

	printf("Chunk %06"PRIu64 ": ", chunk_count);

	printf("%.2hhx", hash[0]);
	for (j = 1; j < hash_size_in_bytes; j++)
		printf(":%.2hhx", hash[j]);
	printf("\n");
}

void send_one_trace(sds path) {
	char buf[MAXLINE];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;
	uint64_t chunk_count;
	time_t scan_start_time;
	int ret;

	handle = hashfile_open(path);
	if (!handle) {
		fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(1);
	}

	/* Print some information about the hash file */
	scan_start_time = hashfile_start_time(handle);
	printf("Collected at [%s] on %s",
			hashfile_sysid(handle),
			ctime(&scan_start_time));

	ret = hashfile_chunking_method_str(handle, buf, MAXLINE);
	if (ret < 0) {
		fprintf(stderr, "Unrecognized chunking method: %d!", errno);
        exit(1);
	}

	printf("Chunking method: %s", buf);

	ret = hashfile_hashing_method_str(handle, buf, MAXLINE);
	if (ret < 0) {
		fprintf(stderr, "Unrecognized hashing method: %d!", errno);
        exit(1);
	}

	printf("Hashing method: %s\n", buf);

	/* Go over the files in a hashfile */
	while (1) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		ret = hashfile_next_file(handle);

		TIMER_END(1, jcr.read_time);

		if (ret < 0) {
			fprintf(stderr,
				"Cannot get next file from a hashfile: %d!\n",
				errno);
            exit(1);
		}

		/* exit the loop if it was the last file */
		if (ret == 0)
			break;

        struct chunk* c = new_chunk(strlen(hashfile_curfile_path(handle))+1);
        strcpy(c->data, hashfile_curfile_path(handle));

		VERBOSE("Read trace phase: %s", c->data);

		SET_CHUNK(c, CHUNK_FILE_START);

		sync_queue_push(trace_queue, c);

		/* Go over the chunks in the current file */
		chunk_count = 0;
		while (1) {
		    TIMER_BEGIN(1);
			ci = hashfile_next_chunk(handle);
		    TIMER_END(1, jcr.read_time);
            
			if (!ci) /* exit the loop if it was the last chunk */
				break;

			chunk_count++;

            c = new_chunk(0);
            c->size = ci->size;
            /*
             * Need some padding.
             */
            memset(c->fp, 0, sizeof(fingerprint));
            memcpy(c->fp, ci->hash, hashfile_hash_size(handle) / 8);

			sync_queue_push(trace_queue, c);

		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(trace_queue, c);

	}
	hashfile_close(handle);
}

static void find_file_in_list(sds config_path) {
    char line[256];
	FILE *file = fopen(config_path, "r");
    if (file == NULL) {
		fprintf(stderr, "cannot find %s\n", config_path);
		exit(1);
    }

    while (fgets(line, sizeof(line), file)) {
        if (line[strlen(line) - 1] == '\n') {
            line[strlen(line) - 1] = '\0';
        }
		sds path = sdsnew(line);
		send_one_trace(path);
		sdsfree(path);
    }
    fclose(file);
}

void* read_fsl_trace(void *argv)
{
	struct stat st;
	stat(jcr.path, &st);
	if (S_ISDIR(st.st_mode)) {
		assert(0); // not implemented
	} else if (S_ISREG(st.st_mode)) {
		if (strstr(jcr.path, ".txt")) {
			find_file_in_list(jcr.path);
		} else {
			send_one_trace(jcr.path);
		}
	} else {
		fprintf(stderr, "The path %s is not a regular file or directory\n", jcr.path);
		exit(1);
	}
	sync_queue_term(trace_queue);

    return NULL;
}

