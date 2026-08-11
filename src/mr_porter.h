#ifndef MR_PORTER_H
#define MR_PORTER_H

#include "mr_cxlfork.h"
#include "mr_util.h"
#include "mr_wire.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum mr_launch_mode {
    MR_LAUNCH_PERSISTENT = 0,
    MR_LAUNCH_LOCALFORK = 1,
    MR_LAUNCH_LOCAL_PREFORK = 2,
    MR_LAUNCH_CXLFORK = 3,
} mr_launch_mode;

typedef int (*mr_porter_task_fn)(void *context, uint32_t worker_id,
                                 uint32_t kind, uint64_t task_id,
                                 uint64_t partition_id,
                                 uint64_t partition_count,
                                 const MR_WorkerStateV1 *worker_state,
                                 mr_template_binding_result *binding,
                                 const void *payload, size_t payload_len,
                                 byte_buf *response,
                                 mr_task_trace_v3 *trace);

typedef struct mr_porter_config {
    mr_launch_mode mode;
    uint32_t workers;
    uint64_t slot_bytes;
    mr_template_options template;
    mr_cxlfork_restore_options restore;
    uint32_t restore_hold_ms;
    bool restore_preopen;
    mr_porter_task_fn task_fn;
    void *task_context;
} mr_porter_config;

typedef struct mr_porter_result {
    uint32_t status;
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
    uint64_t total_ns;
    uint32_t trace_flags;
    mr_task_trace_v3 trace;
    mr_cxlfork_timing restore_timing;
    mr_template_binding_result binding;
    mr_cxlfork_result restore;
    mr_worker_spawn_trace_v1 executed_worker;
    mr_worker_spawn_trace_v1 replacement_worker;
} mr_porter_result;

typedef struct mr_porter_stop_trace_v1 {
    uint64_t stop_begin_ns;
    uint64_t stop_command_sent_ns;
    uint64_t supervisor_stop_received_ns;
    uint64_t workers_stop_begin_ns;
    uint64_t workers_stop_end_ns;
    uint64_t stop_reply_received_ns;
    uint64_t supervisor_wait_begin_ns;
    uint64_t supervisor_wait_end_ns;
    uint64_t stop_end_ns;
} mr_porter_stop_trace_v1;

typedef struct mr_porter mr_porter;

const char *mr_launch_mode_name(mr_launch_mode mode);
int mr_launch_mode_parse(const char *text, mr_launch_mode *mode);
int mr_porter_start(const mr_porter_config *config, mr_porter **out);
int mr_porter_start_timed(const mr_porter_config *config, mr_porter **out,
                          mr_porter_start_trace_v1 *trace);
int mr_porter_submit(mr_porter *porter, uint32_t worker_id, uint32_t kind,
                     uint64_t task_id, uint64_t partition_id,
                     uint64_t partition_count, const void *payload,
                     size_t payload_len, uint64_t agent_request_received_ns,
                     uint64_t agent_parse_done_ns);
int mr_porter_collect(mr_porter *porter, uint32_t *worker_id,
                      byte_buf *response, mr_porter_result *result);
int mr_porter_call(mr_porter *porter, uint32_t worker_id, uint32_t kind,
                   uint64_t task_id, uint64_t partition_id,
                   uint64_t partition_count, const void *payload,
                   size_t payload_len, byte_buf *response,
                   mr_porter_result *result);
void mr_porter_stop(mr_porter *porter);
void mr_porter_stop_timed(mr_porter *porter, mr_porter_stop_trace_v1 *trace);

#endif
