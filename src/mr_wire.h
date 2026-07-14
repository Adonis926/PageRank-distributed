#ifndef MR_WIRE_H
#define MR_WIRE_H

#include "mr_util.h"

#include <stddef.h>
#include <stdint.h>

#define MR_OP 100U

typedef enum mr_task_kind {
    MR_TASK_MAP = 1,
    MR_TASK_REDUCE = 2,
    MR_TASK_SHUTDOWN = 3,
    MR_TASK_ERROR = 4,
} mr_task_kind;

typedef struct mr_wire_header {
    uint32_t magic;
    uint32_t version;
    uint32_t kind;
    uint32_t reserved;
    uint64_t task_id;
    uint64_t partition_id;
    uint64_t partition_count;
    uint64_t payload_len;
} mr_wire_header;

byte_buf mr_wire_make(mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *payload, size_t payload_len);
int mr_wire_parse(const uint8_t *payload, uint32_t len, mr_wire_header *hdr,
                  const char **body, size_t *body_len);

#endif
