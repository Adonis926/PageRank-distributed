#ifndef MR_WIRE_H
#define MR_WIRE_H

#include "mr_util.h"

#include <stddef.h>
#include <stdint.h>

#define MR_OP 100U
#define MR_TASK_RESULT_MAGIC 0x4d525431U
#define MR_TASK_RESULT_VERSION_V2 2U
#define MR_TASK_RESULT_VERSION 3U

#define MR_TRACE_F_WARMUP (1U << 0)
#define MR_TRACE_F_ACTIVATION_PROBE (1U << 1)
#define MR_RESTORE_SETUP_PER_REQUEST 0U
#define MR_RESTORE_SETUP_PREOPEN 1U

#define SHUFFLE_CHUNK_FIRST 0x1U
#define SHUFFLE_CHUNK_LAST 0x2U

typedef enum mr_task_kind {
    MR_TASK_MAP = 1,
    MR_TASK_REDUCE = 2,
    MR_TASK_SHUTDOWN = 3,
    MR_TASK_ERROR = 4,
    MR_TASK_SHUFFLE_FETCH = 5,
    MR_TASK_ACTIVATION_PROBE = 6,
} mr_task_kind;

typedef struct mr_activation_probe_request {
    uint32_t target_worker_id;
    uint32_t expected_role;
    uint32_t flags;
    uint32_t reserved0;
    uint64_t sequence;
    uint64_t expected_state_checksum;
    uint64_t reserved1[2];
} mr_activation_probe_request;

typedef struct mr_activation_probe_response {
    uint64_t sequence;
    uint64_t state_checksum;
    uint64_t proof;
    uint64_t access_count;
} mr_activation_probe_response;

typedef struct mr_task_trace_v3 {
    uint64_t agent_request_received_ns;
    uint64_t agent_parse_done_ns;
    uint64_t porter_submit_begin_ns;
    uint64_t payload_copy_done_ns;
    uint64_t slot_ready_publish_ns;
    uint64_t supervisor_command_received_ns;
    uint64_t fork_begin_ns;
    uint64_t fork_return_ns;
    uint64_t worker_slot_observed_ns;
    uint64_t worker_entry_ns;
    uint64_t restore_wrapper_begin_ns;
    uint64_t restore_wrapper_end_ns;
    uint64_t resume_ioctl_begin_ns;
    uint64_t resume_ioctl_end_ns;
    uint64_t restore_postcheck_done_ns;
    uint64_t template_bind_begin_ns;
    uint64_t template_bind_end_ns;
    uint64_t state_ready_ns;
    uint64_t business_entry_ns;
    uint64_t first_state_access_ns;
    uint64_t first_callback_entry_ns;
    uint64_t callback_region_end_ns;
    uint64_t shuffle_begin_ns;
    uint64_t shuffle_end_ns;
    uint64_t parse_sort_begin_ns;
    uint64_t parse_sort_end_ns;
    uint64_t serialize_begin_ns;
    uint64_t serialize_end_ns;
    uint64_t task_done_ns;
    uint64_t restore_collect_begin_ns;
    uint64_t restore_collect_end_ns;
    uint64_t slot_done_publish_ns;
    uint64_t supervisor_done_observed_ns;
    uint64_t reap_begin_ns;
    uint64_t reap_end_ns;
    uint64_t refill_begin_ns;
    uint64_t refill_end_ns;
    uint64_t pool_ready_ns;
    uint64_t agent_collect_done_ns;
    uint64_t response_encode_done_ns;
    uint64_t response_publish_begin_ns;
    uint64_t response_publish_end_ns;
} mr_task_trace_v3;

typedef struct mr_kernel_restore_stats_v3 {
    uint64_t supported_mask;
    uint64_t valid_mask;
    uint64_t path_mask;
    uint64_t total_ns;
    uint64_t lock_wait_ns;
    uint64_t lifecycle_ns;
    uint64_t manifest_validate_ns;
    uint64_t image_validate_ns;
    uint64_t checksum_validate_ns;
    uint64_t address_conflict_ns;
    uint64_t vma_create_ns;
    uint64_t upper_pt_alloc_ns;
    uint64_t shared_leaf_attach_ns;
    uint64_t pte_image_validate_ns;
    uint64_t direct_pte_install_ns;
    uint64_t fallback_other_ns;
    uint64_t dirty_prefault_ns;
    uint64_t global_restore_ns;
    uint64_t other_ns;
    uint64_t faults;
    uint64_t fault_dispatch_ns;
    uint64_t leaf_materialize_ns;
    uint64_t data_cow_ns;
    uint64_t cxl_to_local_copy_ns;
    uint64_t shared_leaf_count;
    uint64_t shared_data_pages;
    uint64_t fallback_pages;
    uint64_t direct_pte_install_pages;
    uint64_t local_copy_pages;
    uint64_t leaf_copy_pages;
    uint64_t dirty_prefault_pages;
    uint64_t validated_ptes;
} mr_kernel_restore_stats_v3;

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

typedef struct mr_task_result_v3 {
    uint32_t magic;
    uint16_t size;
    uint16_t version;
    uint32_t kind;
    uint32_t status;
    uint32_t node_id;
    uint32_t worker_id;
    uint64_t task_id;
    int64_t worker_pid;
    uint32_t launch_mode;
    uint32_t policy;
    uint32_t validate_full;
    uint32_t trace_flags;
    mr_task_trace_v3 trace;
    uint64_t restore_wrapper_ns;
    uint64_t session_alloc_ns;
    uint64_t device_open_ns;
    uint64_t query_caps_ns;
    uint64_t policy_check_ns;
    uint64_t resume_ioctl_ns;
    uint64_t restore_postcheck_ns;
    uint64_t preopen_device_ns;
    uint64_t preopen_caps_ns;
    uint64_t preopen_policy_ns;
    uint64_t restore_setup_mode;
    mr_kernel_restore_stats_v3 kernel;
    uint64_t template_key;
    uint64_t template_generation;
    uint64_t restore_id;
    uint64_t restore_path_mask;
    uint64_t total_pages;
    uint64_t page_checksum;
    uint64_t template_state_checksum;
    uint64_t template_access_checksum;
    uint64_t template_access_count;
    uint64_t template_state_pages;
    uint64_t template_binding_flags;
    uint64_t reserved[8];
} mr_task_result_v3;

_Static_assert(sizeof(ShuffleChunkHeader) == 40,
               "ShuffleChunkHeader wire size must stay stable");
_Static_assert(sizeof(mr_task_result_v2) == 232,
               "mr_task_result_v2 wire size must stay stable");
_Static_assert(sizeof(mr_task_result_v3) == 888,
               "mr_task_result_v3 wire size must stay stable");

byte_buf mr_wire_make(mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *payload, size_t payload_len);
int mr_wire_parse(const uint8_t *payload, uint32_t len, mr_wire_header *hdr,
                  const char **body, size_t *body_len);

#endif
