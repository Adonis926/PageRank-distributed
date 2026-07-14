#ifndef MR_KV_H
#define MR_KV_H

#include "mr_util.h"

#include <stdbool.h>
#include <stddef.h>

typedef struct mr_kv_pair {
    char *key;
    char *value;
} mr_kv_pair;

typedef struct mr_partition {
    mr_kv_pair *pairs;
    size_t len;
    size_t cap;
    size_t cursor;
} mr_partition;

typedef struct mr_partitions {
    mr_partition *items;
    int count;
} mr_partitions;

void mr_partitions_init(mr_partitions *partitions, int count);
void mr_partitions_destroy(mr_partitions *partitions);
void mr_partitions_emit(mr_partitions *partitions, int partition_number,
                        const char *key, const char *value);
void mr_partitions_sort(mr_partitions *partitions);
char *mr_partitions_get_next(mr_partitions *partitions, char *key,
                             int partition_number);
int mr_partitions_serialize(const mr_partitions *partitions, byte_buf *out,
                            size_t payload_limit);
int mr_partition_serialize(const mr_partitions *partitions, int partition_number,
                           byte_buf *out, size_t payload_limit);
int mr_partitions_parse(mr_partitions *partitions, const char *data, size_t len,
                        bool use_wire_partition, int fixed_partition);
int mr_partitions_write_text(const mr_partitions *partitions, const char *path);

#endif
