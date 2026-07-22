#ifndef MR_HYDRA_TRANSPORT_H
#define MR_HYDRA_TRANSPORT_H

#include "hydra/hydra.h"
#include "mr_util.h"

#include <stdbool.h>
#include <stdint.h>

typedef enum mr_backend {
    MR_BACKEND_FILE,
    MR_BACKEND_DEVDAX,
    MR_BACKEND_IVSHMEM,
} mr_backend;

typedef struct mr_endpoint {
    const char *path;
    hydra_ctx *ctx;
    bool busy;
    uint64_t task_id;
} mr_endpoint;

typedef enum mr_queue_policy {
    MR_QUEUE_SHARED_PAIR,
    MR_QUEUE_PER_CONNECTION,
} mr_queue_policy;

typedef struct mr_hydra_open_config {
    mr_backend backend;
    const char *path;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t connections;
    uint64_t connection_id;
    uint64_t node_id;
    uint64_t peer_node_id;
    uint64_t service_id;
    mr_queue_policy queue_policy;
    bool create;
} mr_hydra_open_config;

typedef struct mr_hydra_send_observation {
    hydra_send_timing phases;
    uint64_t call_begin_ns;
    uint64_t publish_done_ns;
    uint64_t wait_ns;
    uint64_t retries;
    uint32_t queue_was_empty;
    uint32_t reserved;
} mr_hydra_send_observation;

mr_backend mr_backend_parse(const char *value);
const char *mr_backend_name(mr_backend backend);
mr_queue_policy mr_queue_policy_parse(const char *value);
const char *mr_queue_policy_name(mr_queue_policy policy);
int mr_hydra_open(hydra_ctx **ctx, mr_backend backend, const char *path,
                  uint64_t map_size, uint64_t queue_capacity,
                  uint64_t payload_max, bool create);
int mr_hydra_open_ex(hydra_ctx **ctx, const mr_hydra_open_config *config);
int mr_hydra_send_request_poll(hydra_ctx *ctx, uint64_t req_id,
                               const byte_buf *payload);
int mr_hydra_send_request_poll_observed(
    hydra_ctx *ctx, uint64_t req_id, const byte_buf *payload,
    mr_hydra_send_observation *observation);
int mr_hydra_send_response_poll(hydra_ctx *ctx, const hydra_msg *request,
                                uint32_t status, const byte_buf *payload);
int mr_hydra_send_response_poll_observed(
    hydra_ctx *ctx, const hydra_msg *request, uint32_t status,
    const byte_buf *payload, mr_hydra_send_observation *observation);
int mr_hydra_recv_request_poll(hydra_ctx *ctx, uint64_t *offset,
                               const hydra_msg **msg, const uint8_t **payload);
int mr_hydra_recv_response(hydra_ctx *ctx, uint64_t *offset,
                           const hydra_msg **msg, const uint8_t **payload);

#endif
