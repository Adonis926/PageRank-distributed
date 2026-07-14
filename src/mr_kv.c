#include "mr_kv.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct mr_kv_wire_entry {
    uint64_t partition_id;
    uint64_t key_len;
    uint64_t value_len;
} mr_kv_wire_entry;

static void ensure_capacity(mr_partition *partition)
{
    if (partition->len < partition->cap) {
        return;
    }

    size_t cap = partition->cap ? partition->cap * 2 : 16;
    partition->pairs = mr_xrealloc(partition->pairs,
                                   cap * sizeof(partition->pairs[0]));
    partition->cap = cap;
}

static int pair_compare(const void *left, const void *right)
{
    const mr_kv_pair *a = left;
    const mr_kv_pair *b = right;
    int key_cmp = strcmp(a->key, b->key);

    if (key_cmp != 0) {
        return key_cmp;
    }

    return strcmp(a->value, b->value);
}

void mr_partitions_init(mr_partitions *partitions, int count)
{
    partitions->items = mr_xcalloc((size_t)count, sizeof(partitions->items[0]));
    partitions->count = count;
}

void mr_partitions_destroy(mr_partitions *partitions)
{
    for (int i = 0; i < partitions->count; i++) {
        mr_partition *partition = &partitions->items[i];
        for (size_t j = 0; j < partition->len; j++) {
            free(partition->pairs[j].key);
            free(partition->pairs[j].value);
        }
        free(partition->pairs);
    }

    free(partitions->items);
    partitions->items = NULL;
    partitions->count = 0;
}

void mr_partitions_emit(mr_partitions *partitions, int partition_number,
                        const char *key, const char *value)
{
    if (partition_number < 0 || partition_number >= partitions->count) {
        fprintf(stderr, "invalid partition number: %d\n", partition_number);
        exit(1);
    }

    mr_partition *partition = &partitions->items[partition_number];
    ensure_capacity(partition);
    partition->pairs[partition->len].key = mr_xstrdup(key);
    partition->pairs[partition->len].value = mr_xstrdup(value);
    partition->len++;
}

void mr_partitions_sort(mr_partitions *partitions)
{
    for (int i = 0; i < partitions->count; i++) {
        mr_partition *partition = &partitions->items[i];
        qsort(partition->pairs, partition->len, sizeof(partition->pairs[0]),
              pair_compare);
        partition->cursor = 0;
    }
}

char *mr_partitions_get_next(mr_partitions *partitions, char *key,
                             int partition_number)
{
    if (partition_number < 0 || partition_number >= partitions->count) {
        return NULL;
    }

    mr_partition *partition = &partitions->items[partition_number];
    if (partition->cursor >= partition->len) {
        return NULL;
    }

    mr_kv_pair *pair = &partition->pairs[partition->cursor];
    if (strcmp(pair->key, key) != 0) {
        return NULL;
    }

    partition->cursor++;
    return pair->value;
}

static int serialize_pair(byte_buf *out, uint64_t partition_id,
                          const mr_kv_pair *pair, size_t payload_limit)
{
    mr_kv_wire_entry entry = {
        .partition_id = partition_id,
        .key_len = strlen(pair->key),
        .value_len = strlen(pair->value),
    };
    size_t need = sizeof(entry) + (size_t)entry.key_len + (size_t)entry.value_len;

    if (payload_limit > 0 && out->len + need > payload_limit) {
        return EMSGSIZE;
    }

    byte_buf_append(out, &entry, sizeof(entry));
    byte_buf_append(out, pair->key, (size_t)entry.key_len);
    byte_buf_append(out, pair->value, (size_t)entry.value_len);
    return 0;
}

int mr_partitions_serialize(const mr_partitions *partitions, byte_buf *out,
                            size_t payload_limit)
{
    for (int i = 0; i < partitions->count; i++) {
        const mr_partition *partition = &partitions->items[i];
        for (size_t j = 0; j < partition->len; j++) {
            int err = serialize_pair(out, (uint64_t)i, &partition->pairs[j],
                                     payload_limit);
            if (err != 0) {
                return err;
            }
        }
    }
    return 0;
}

int mr_partition_serialize(const mr_partitions *partitions, int partition_number,
                           byte_buf *out, size_t payload_limit)
{
    if (partition_number < 0 || partition_number >= partitions->count) {
        return EINVAL;
    }

    const mr_partition *partition = &partitions->items[partition_number];
    for (size_t i = 0; i < partition->len; i++) {
        int err = serialize_pair(out, (uint64_t)partition_number,
                                 &partition->pairs[i], payload_limit);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

int mr_partitions_parse(mr_partitions *partitions, const char *data, size_t len,
                        bool use_wire_partition, int fixed_partition)
{
    size_t pos = 0;
    while (pos < len) {
        mr_kv_wire_entry entry = {0};
        if (len - pos < sizeof(entry)) {
            return EINVAL;
        }

        memcpy(&entry, data + pos, sizeof(entry));
        pos += sizeof(entry);

        if (entry.key_len > SIZE_MAX || entry.value_len > SIZE_MAX) {
            return EINVAL;
        }

        size_t key_len = (size_t)entry.key_len;
        size_t value_len = (size_t)entry.value_len;
        if (key_len > len - pos || value_len > len - pos - key_len) {
            return EINVAL;
        }

        int partition_number = fixed_partition;
        if (use_wire_partition) {
            if (entry.partition_id >= (uint64_t)partitions->count) {
                return EINVAL;
            }
            partition_number = (int)entry.partition_id;
        }

        char *key = mr_xstrndup(data + pos, key_len);
        pos += key_len;
        char *value = mr_xstrndup(data + pos, value_len);
        pos += value_len;

        mr_partitions_emit(partitions, partition_number, key, value);
        free(key);
        free(value);
    }
    return 0;
}

int mr_partitions_write_text(const mr_partitions *partitions, const char *path)
{
    FILE *fp = fopen(path, "wb");
    if (fp == NULL) {
        fprintf(stderr, "open output %s failed: %s\n", path, strerror(errno));
        return 1;
    }

    for (int i = 0; i < partitions->count; i++) {
        const mr_partition *partition = &partitions->items[i];
        for (size_t j = 0; j < partition->len; j++) {
            if (fprintf(fp, "%s\t%s\n", partition->pairs[j].key,
                        partition->pairs[j].value) < 0) {
                fprintf(stderr, "write output failed\n");
                fclose(fp);
                return 1;
            }
        }
    }

    fclose(fp);
    return 0;
}
