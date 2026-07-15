#ifndef MR_WIRE_H
#define MR_WIRE_H

#include "mr_util.h"

#include <stddef.h>
#include <stdint.h>

#define MR_OP 100U

#define SHUFFLE_CHUNK_FIRST 0x1U
#define SHUFFLE_CHUNK_LAST 0x2U

typedef enum mr_task_kind {
    MR_TASK_MAP = 1,
    MR_TASK_REDUCE = 2,
    MR_TASK_SHUTDOWN = 3,
    MR_TASK_ERROR = 4,
    MR_TASK_SHUFFLE_FETCH = 5,
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

typedef struct ShuffleChunkHeader {
    uint64_t job_id;
    uint32_t mapper_id;
    uint32_t reducer_id;

    uint64_t partition_seq;
    uint32_t chunk_seq;
    uint32_t flags;

    uint32_t record_count;
    uint32_t payload_bytes;
} ShuffleChunkHeader;

_Static_assert(sizeof(ShuffleChunkHeader) == 40,
               "ShuffleChunkHeader wire size must stay stable");

byte_buf mr_wire_make(mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *payload, size_t payload_len);
int mr_wire_parse(const uint8_t *payload, uint32_t len, mr_wire_header *hdr,
                  const char **body, size_t *body_len);

#endif
