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

mr_backend mr_backend_parse(const char *value);
const char *mr_backend_name(mr_backend backend);
int mr_hydra_open(hydra_ctx **ctx, mr_backend backend, const char *path,
                  uint64_t map_size, uint64_t queue_capacity,
                  uint64_t payload_max, bool create);
int mr_hydra_send_request_poll(hydra_ctx *ctx, uint64_t req_id,
                               const byte_buf *payload);
int mr_hydra_send_response_poll(hydra_ctx *ctx, const hydra_msg *request,
                                uint32_t status, const byte_buf *payload);
int mr_hydra_recv_request_poll(hydra_ctx *ctx, uint64_t *offset,
                               const hydra_msg **msg, const uint8_t **payload);
int mr_hydra_recv_response(hydra_ctx *ctx, uint64_t *offset,
                           const hydra_msg **msg, const uint8_t **payload);

#endif
