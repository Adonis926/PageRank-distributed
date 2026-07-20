#ifndef MR_WIRE_H
#define MR_WIRE_H

#include "mr_util.h"

#include <stddef.h>
#include <stdint.h>

#define MR_OP 100U
#define MR_TASK_RESULT_MAGIC 0x4d525431U
#define MR_TASK_RESULT_VERSION 2U

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

typedef struct mr_task_result_v2 {
    uint32_t magic;
    uint16_t size;
    uint16_t version;
    uint32_t kind;
    uint32_t status;
    uint32_t node_id;
    uint32_t worker_id;
    uint64_t task_id;
    int64_t worker_pid;
    uint64_t dispatch_ns;
    uint64_t fork_ns;
    uint64_t wakeup_ns;
    uint64_t restore_ioctl_ns;
    uint64_t template_validate_ns;
    uint64_t task_ns;
    uint64_t reap_ns;
    uint64_t refill_ns;
    uint64_t porter_total_ns;
    uint64_t restore_id;
    uint64_t restore_path_mask;
    uint64_t kernel_total_ns;
    uint64_t shared_leaf_pages;
    uint64_t fallback_pages;
    uint64_t local_copy_pages;
    uint64_t leaf_copy_pages;
    uint64_t template_state_checksum;
    uint64_t template_access_checksum;
    uint64_t template_access_count;
    uint64_t template_state_pages;
    uint64_t template_binding_flags;
    uint64_t reserved[3];
} mr_task_result_v2;

_Static_assert(sizeof(ShuffleChunkHeader) == 40,
               "ShuffleChunkHeader wire size must stay stable");
_Static_assert(sizeof(mr_task_result_v2) == 232,
               "mr_task_result_v2 wire size must stay stable");

byte_buf mr_wire_make(mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *payload, size_t payload_len);
int mr_wire_parse(const uint8_t *payload, uint32_t len, mr_wire_header *hdr,
                  const char **body, size_t *body_len);

#endif
