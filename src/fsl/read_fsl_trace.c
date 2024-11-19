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
static uint64_t fileCount = 0;

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
	WARNING("Trace file: %s", path);
    struct chunk *c = new_chunk(strlen(path) + 1);
    strcpy(c->data, path);
    SET_CHUNK(c, CHUNK_FILE_START);
    sync_queue_push(trace_queue, c);

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

        // struct chunk* c = new_chunk(strlen(hashfile_curfile_path(handle))+1);
        // strcpy(c->data, hashfile_curfile_path(handle));
        // struct chunk* c = new_chunk(8+1);
        // snprintf(c->data, 8, "%llx", fileCount++);
		// VERBOSE("Read trace phase: %s", c->data);
		// SET_CHUNK(c, CHUNK_FILE_START);
		// sync_queue_push(trace_queue, c);

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
			if (hashfile_hash_size(handle) != 48) {
				WARNING("hash size is not 48: %d", hashfile_hash_size(handle));
			}

			sync_queue_push(trace_queue, c);

		}
	}
    c = new_chunk(0);
    SET_CHUNK(c, CHUNK_FILE_END);
    sync_queue_push(trace_queue, c);
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

static void trace_thread(sds path)
{
    DIR *dir = opendir(path);
    struct dirent *entry;
    struct stat stbuf;

    while (entry = readdir(dir)) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;

        sds file_path = sdsdup(path);
        file_path = sdscat(file_path, "/");
        file_path = sdscat(file_path, entry->d_name);
		WARNING("Trace file: %s", entry->d_name);
        stat(file_path, &stbuf);
        if (S_ISDIR(stbuf.st_mode)) {
            sdsfree(file_path);
            continue;
        }

        struct hashfile_handle *fp = hashfile_open(file_path);
        if (!fp) {
            printf("Fail to open trace file : %s\n", file_path);
            sdsfree(file_path);
            continue;
        }
        sdsfree(file_path);

        const struct chunk_info *cinfo;
        while (hashfile_next_file(fp) == 1) {

            // struct chunk *c = new_chunk(strlen(hashfile_curfile_path(fp)) + 1);
            // strcpy(c->data, hashfile_curfile_path(fp));

			struct chunk* c = new_chunk(8+1);
			snprintf(c->data, 8, "%llx", fileCount++);

            SET_CHUNK(c, CHUNK_FILE_START);

            sync_queue_push(trace_queue, c);

            while (cinfo = hashfile_next_chunk(fp)) {
                c = new_chunk(0);
                c->size = cinfo->size;
                // fpcpy(c->fp, cinfo->hash, 1);
				memset(c->fp, 0, sizeof(fingerprint));
				memcpy(c->fp, cinfo->hash, hashfile_hash_size(fp) / 8);
				if (hashfile_hash_size(fp) != 48) {
					WARNING("hash size is not 48: %d", hashfile_hash_size(fp));
				}

                sync_queue_push(trace_queue, c);
            }

            c = new_chunk(0);
            SET_CHUNK(c, CHUNK_FILE_END);

            sync_queue_push(trace_queue, c);
        }

        hashfile_close(fp);
    }

    closedir(dir);
}

void* read_fsl_trace(void *argv)
{
    struct stat stbuf;
    if (stat(jcr.path, &stbuf)) {
        WARNING("%s does not exist!\n", jcr.path);
        exit(1);
    }
    if (S_ISREG(stbuf.st_mode)) {
        find_file_in_list(jcr.path);
    } else if (S_ISDIR(stbuf.st_mode)) {
	    trace_thread(jcr.path);
    } else {
        WARNING("Trace file is not a regular file or a directory\n");
        exit(1);
    }
	sync_queue_term(trace_queue);

    return NULL;
}

