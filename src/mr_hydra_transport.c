#include "mr_hydra_transport.h"

#include "mr_wire.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

mr_backend mr_backend_parse(const char *value)
{
    if (strcmp(value, "file") == 0) {
        return MR_BACKEND_FILE;
    }
    if (strcmp(value, "devdax") == 0) {
        return MR_BACKEND_DEVDAX;
    }
    if (strcmp(value, "ivshmem") == 0) {
        return MR_BACKEND_IVSHMEM;
    }

    fprintf(stderr, "invalid backend: %s\n", value);
    exit(2);
}

const char *mr_backend_name(mr_backend backend)
{
    switch (backend) {
    case MR_BACKEND_FILE:
        return "file";
    case MR_BACKEND_DEVDAX:
        return "devdax";
    case MR_BACKEND_IVSHMEM:
        return "ivshmem";
    default:
        return "unknown";
    }
}

mr_queue_policy mr_queue_policy_parse(const char *value)
{
    if (strcmp(value, "shared_pair") == 0) {
        return MR_QUEUE_SHARED_PAIR;
    }
    if (strcmp(value, "per_connection") == 0) {
        return MR_QUEUE_PER_CONNECTION;
    }

    fprintf(stderr, "invalid queue policy: %s\n", value);
    exit(2);
}

const char *mr_queue_policy_name(mr_queue_policy policy)
{
    switch (policy) {
    case MR_QUEUE_SHARED_PAIR:
        return "shared_pair";
    case MR_QUEUE_PER_CONNECTION:
        return "per_connection";
    default:
        return "unknown";
    }
}

static hydra_queue_policy to_hydra_queue_policy(mr_queue_policy policy)
{
    return policy == MR_QUEUE_PER_CONNECTION ? HYDRA_QUEUE_POLICY_PER_CONNECTION
                                             : HYDRA_QUEUE_POLICY_SHARED_PAIR;
}

int mr_hydra_open_ex(hydra_ctx **ctx, const mr_hydra_open_config *config)
{
    hydra_open_options opts = {
        .path = config->path,
        .map_size = config->map_size,
        .queue_capacity = config->queue_capacity,
        .payload_max = config->payload_max,
        .connections = config->connections ? config->connections : 1,
        .connection_id = config->connection_id,
        .node_id = config->node_id,
        .peer_node_id = config->peer_node_id,
        .service_id = config->service_id,
        .queue_policy = to_hydra_queue_policy(config->queue_policy),
        .create = config->create,
        .truncate_file = config->create && config->backend == MR_BACKEND_FILE,
        .checksum_enabled = true,
    };

    switch (config->backend) {
    case MR_BACKEND_FILE:
        return hydra_open_file(ctx, &opts);
    case MR_BACKEND_DEVDAX:
        return hydra_open_devdax(ctx, &opts);
    case MR_BACKEND_IVSHMEM:
        return hydra_open_ivshmem(ctx, &opts);
    default:
        return EINVAL;
    }
}

int mr_hydra_open(hydra_ctx **ctx, mr_backend backend, const char *path,
                  uint64_t map_size, uint64_t queue_capacity,
                  uint64_t payload_max, bool create)
{
    mr_hydra_open_config config = {
        .backend = backend,
        .path = path,
        .map_size = map_size,
        .queue_capacity = queue_capacity,
        .payload_max = payload_max,
        .connections = 1,
        .connection_id = 0,
        .node_id = 0,
        .peer_node_id = 1,
        .service_id = 1,
        .queue_policy = MR_QUEUE_PER_CONNECTION,
        .create = create,
    };
    return mr_hydra_open_ex(ctx, &config);
}

int mr_hydra_send_request_poll(hydra_ctx *ctx, uint64_t req_id,
                               const byte_buf *payload)
{
    if (payload->len > UINT32_MAX) {
        return EMSGSIZE;
    }

    int err = 0;
    do {
        err = hydra_send_request(ctx, req_id, MR_OP, payload->data,
                                 (uint32_t)payload->len, hydra_now_ns());
        if (err == EAGAIN) {
            hydra_wait(ctx, HYDRA_WAIT_REQUEST_SPACE, HYDRA_NOTIFY_BUSY);
        }
    } while (err == EAGAIN);
    return err;
}

int mr_hydra_send_response_poll(hydra_ctx *ctx, const hydra_msg *request,
                                uint32_t status, const byte_buf *payload)
{
    if (payload->len > UINT32_MAX) {
        return EMSGSIZE;
    }

    int err = 0;
    do {
        err = hydra_send_response(ctx, request, status, payload->data,
                                  (uint32_t)payload->len);
        if (err == EAGAIN) {
            hydra_wait(ctx, HYDRA_WAIT_RESPONSE_SPACE, HYDRA_NOTIFY_BUSY);
        }
    } while (err == EAGAIN);
    return err;
}

int mr_hydra_recv_request_poll(hydra_ctx *ctx, uint64_t *offset,
                               const hydra_msg **msg, const uint8_t **payload)
{
    int err = 0;
    do {
        err = hydra_recv_request(ctx, offset, msg, payload);
        if (err == EAGAIN) {
            hydra_wait(ctx, HYDRA_WAIT_REQUEST, HYDRA_NOTIFY_BUSY);
        }
    } while (err == EAGAIN);
    return err;
}

int mr_hydra_recv_response(hydra_ctx *ctx, uint64_t *offset,
                           const hydra_msg **msg, const uint8_t **payload)
{
    return hydra_recv_response(ctx, offset, msg, payload);
}
