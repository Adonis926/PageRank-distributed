#define _GNU_SOURCE

#include "pr_cxlfork.h"
#include "pr_graph.h"

#include "hydra/hydra.h"

#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#define PR_WIRE_MAGIC UINT64_C(0x5052485944524131)
#define PR_WIRE_VERSION 1U
#define PR_MAX_THREADS 256U
#define PR_DEFAULT_MAP_SIZE (512ULL * 1024ULL * 1024ULL)
#define PR_DEFAULT_PAYLOAD_MAX (64ULL * 1024ULL)
#define PR_DEFAULT_QUEUE_CAPACITY 64ULL
#define PR_SERVICE_ID UINT64_C(0x50524731)
#define PR_OP_ARM 0x50520001U
#define PR_OP_START 0x50520002U
#define PR_OP_DATA 0x50520003U
#define PR_OP_BARRIER 0x50520004U

typedef enum pr_backend {
    PR_BACKEND_FILE,
    PR_BACKEND_IVSHMEM,
    PR_BACKEND_DEVDAX,
} pr_backend;

typedef enum pr_state_mode {
    PR_STATE_LOCAL,
    PR_STATE_CXLFORK,
} pr_state_mode;

typedef struct pr_options {
    pr_backend backend;
    pr_state_mode state_mode;
    const char *tx_path;
    const char *rx_path;
    const char *cpu_list;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t vertices;
    uint32_t degree;
    uint32_t remote_degree;
    uint32_t iterations;
    uint32_t chunks;
    uint32_t threads;
    uint64_t timeout_ms;
    hydra_notify_mode notify_mode;
    bool checksum;
    pr_cxlfork_options cxlfork;
} pr_options;

typedef struct pr_arm_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint64_t run_id;
    uint64_t vertices;
    uint32_t degree;
    uint32_t remote_degree;
    uint32_t iterations;
    uint32_t chunks;
    uint32_t threads;
    uint32_t state_mode;
    uint64_t key;
    uint64_t generation;
    uint64_t page_checksum;
    uint64_t template_base;
    uint64_t template_bytes;
    uint64_t reserved[8];
} pr_arm_v1;

typedef struct pr_arm_result_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint64_t run_id;
    uint64_t arm_total_ns;
    uint64_t graph_ready_ns;
    uint64_t thread_create_ns;
    uint64_t graph_checksum;
    uint64_t restore_wrapper_ns;
    uint64_t restore_ioctl_ns;
    uint64_t restore_kernel_ns;
    uint64_t restore_lifecycle_ns;
    uint64_t restore_validation_ns;
    uint64_t restore_vma_ns;
    uint64_t restore_upper_pt_ns;
    uint64_t restore_leaf_attach_ns;
    uint64_t restore_fallback_ns;
    uint64_t shared_leaf_count;
    uint64_t shared_data_pages;
    uint64_t fallback_pages;
    uint64_t reserved[8];
} pr_arm_result_v1;

typedef struct pr_start_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint64_t run_id;
    uint64_t reserved[4];
} pr_start_v1;

typedef struct pr_node_result_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint64_t run_id;
    uint64_t elapsed_ns;
    uint64_t hydra_send_ns;
    uint64_t hydra_recv_wait_ns;
    uint64_t hydra_release_ns;
    uint64_t messages_sent;
    uint64_t messages_received;
    uint64_t payload_bytes_sent;
    uint64_t payload_bytes_received;
    uint64_t send_retries;
    uint64_t graph_checksum;
    double residual;
    double rank_checksum;
    uint64_t hydra_wait_ns;
    uint64_t hydra_wait_calls;
    uint64_t reserved[6];
} pr_node_result_v1;

typedef struct pr_chunk_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint64_t run_id;
    uint32_t iteration;
    uint32_t tid;
    uint32_t chunk;
    uint32_t value_count;
    uint64_t reserved[2];
} pr_chunk_v1;

typedef struct pr_transport_stats {
    uint64_t send_ns;
    uint64_t recv_wait_ns;
    uint64_t release_ns;
    uint64_t messages_sent;
    uint64_t messages_received;
    uint64_t payload_bytes_sent;
    uint64_t payload_bytes_received;
    uint64_t send_retries;
    uint64_t wait_ns;
    uint64_t wait_calls;
} pr_transport_stats;

typedef struct pr_node_ctx pr_node_ctx;

typedef struct pr_worker_arg {
    pr_node_ctx *ctx;
    uint32_t tid;
} pr_worker_arg;

struct pr_node_ctx {
    uint32_t node;
    pr_options options;
    pr_graph graph;
    hydra_ctx *tx[PR_MAX_THREADS];
    hydra_ctx *rx[PR_MAX_THREADS];
    pthread_t worker[PR_MAX_THREADS];
    pr_worker_arg worker_arg[PR_MAX_THREADS];
    pthread_barrier_t barrier;
    atomic_bool start_gate;
    atomic_int worker_error;
    uint64_t run_id;
    double *rank[2];
    double *partials;
    pr_transport_stats *transport;
    uint64_t elapsed_ns;
    uint64_t graph_checksum;
    double residual;
    double rank_checksum;
};

typedef struct pr_write_chunk_arg {
    pr_node_ctx *ctx;
    uint32_t tid;
    uint32_t iteration;
    uint32_t chunk;
    uint64_t edge_begin;
    uint64_t edge_end;
    const double *source;
} pr_write_chunk_arg;

_Static_assert(sizeof(pr_arm_v1) == 160, "arm wire ABI changed");
_Static_assert(sizeof(pr_arm_result_v1) == 216, "arm result ABI changed");
_Static_assert(sizeof(pr_start_v1) == 56, "start wire ABI changed");
_Static_assert(sizeof(pr_node_result_v1) == 184, "node result ABI changed");
_Static_assert(sizeof(pr_chunk_v1) == 56, "chunk wire ABI changed");

static uint64_t now_ns(void)
{
    return hydra_now_ns();
}

static void print_error(const char *what, int err)
{
    fprintf(stderr, "%s: %s (%d)\n", what, strerror(err), err);
}

static uint64_t parse_u64(const char *text, const char *name)
{
    char *end = NULL;
    unsigned long long value;

    errno = 0;
    value = strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, text);
        exit(2);
    }
    return (uint64_t)value;
}

static uint64_t option_value(const char *arg, const char *prefix,
                             const char **value)
{
    size_t length = strlen(prefix);

    if (strncmp(arg, prefix, length) != 0) {
        return 0;
    }
    *value = arg + length;
    return 1;
}

static const char *backend_name(pr_backend backend)
{
    switch (backend) {
    case PR_BACKEND_FILE:
        return "file";
    case PR_BACKEND_IVSHMEM:
        return "ivshmem";
    case PR_BACKEND_DEVDAX:
        return "devdax";
    }
    return "unknown";
}

static const char *state_name(pr_state_mode mode)
{
    return mode == PR_STATE_CXLFORK ? "cxlfork" : "local";
}

static int parse_cpu_list(const char *text, int *cpus, uint32_t capacity)
{
    char *copy;
    char *save = NULL;
    char *token;
    uint32_t count = 0;

    if (text == NULL || (copy = strdup(text)) == NULL) {
        return EINVAL;
    }
    for (token = strtok_r(copy, ",", &save); token != NULL;
         token = strtok_r(NULL, ",", &save)) {
        int first;
        int last;

        if (sscanf(token, "%d-%d", &first, &last) == 2) {
            if (first < 0 || last < first) {
                free(copy);
                return EINVAL;
            }
        } else if (sscanf(token, "%d", &first) == 1) {
            if (first < 0) {
                free(copy);
                return EINVAL;
            }
            last = first;
        } else {
            free(copy);
            return EINVAL;
        }
        for (int cpu = first; cpu <= last; cpu++) {
            if (count == capacity) {
                free(copy);
                return E2BIG;
            }
            cpus[count++] = cpu;
        }
    }
    free(copy);
    return (int)count;
}

static int pin_thread(int cpu)
{
    cpu_set_t set;

    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    int err = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    return err;
}

static int open_hydra(hydra_ctx **context, const pr_options *options,
                      const char *path, uint64_t connection_id, bool create)
{
    hydra_open_options open_options = {
        .path = path,
        .map_size = options->map_size,
        .queue_capacity = options->queue_capacity,
        .payload_max = options->payload_max,
        .connections = options->threads + 1,
        .connection_id = connection_id,
        .node_id = 0,
        .peer_node_id = 1,
        .service_id = PR_SERVICE_ID,
        .queue_policy = HYDRA_QUEUE_POLICY_PER_CONNECTION,
        .create = create,
        .truncate_file = create && options->backend == PR_BACKEND_FILE,
        .checksum_enabled = options->checksum,
    };

    switch (options->backend) {
    case PR_BACKEND_FILE:
        return hydra_open_file(context, &open_options);
    case PR_BACKEND_IVSHMEM:
        return hydra_open_ivshmem(context, &open_options);
    case PR_BACKEND_DEVDAX:
        return hydra_open_devdax(context, &open_options);
    }
    return EINVAL;
}

static int open_hydra_retry(hydra_ctx **context, const pr_options *options,
                            const char *path, uint64_t connection_id)
{
    uint64_t deadline = now_ns() + options->timeout_ms * UINT64_C(1000000);
    int err;

    do {
        err = open_hydra(context, options, path, connection_id, false);
        if (err == 0) {
            return 0;
        }
        struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};
        nanosleep(&delay, NULL);
    } while (now_ns() < deadline);
    return err;
}

static void wait_hydra(hydra_ctx *context, hydra_wait_target target,
                       hydra_notify_mode mode, pr_transport_stats *stats)
{
    uint64_t begin = now_ns();
    hydra_wait(context, target, mode);
    if (stats != NULL) {
        stats->wait_ns += now_ns() - begin;
        stats->wait_calls++;
    }
}

static int send_request_wait(hydra_ctx *context, uint64_t req_id,
                             uint32_t op, const void *payload, uint32_t length,
                             uint64_t timeout_ms, hydra_notify_mode notify,
                             pr_transport_stats *stats)
{
    uint64_t deadline = now_ns() + timeout_ms * UINT64_C(1000000);
    int err;

    do {
        err = hydra_send_request(context, req_id, op, payload, length,
                                 now_ns());
        if (err == 0) {
            return 0;
        }
        if (err != EAGAIN) {
            return err;
        }
        wait_hydra(context, HYDRA_WAIT_REQUEST_SPACE, notify, stats);
    } while (now_ns() < deadline);
    return ETIMEDOUT;
}

static int recv_request_wait(hydra_ctx *context, uint64_t *offset,
                             const hydra_msg **message,
                             const uint8_t **payload, uint64_t timeout_ms,
                             uint64_t *wait_ns, hydra_notify_mode notify,
                             pr_transport_stats *stats)
{
    uint64_t begin = now_ns();
    uint64_t deadline = begin + timeout_ms * UINT64_C(1000000);
    int err;

    do {
        err = hydra_recv_request(context, offset, message, payload);
        if (err == 0) {
            if (wait_ns != NULL) {
                *wait_ns += now_ns() - begin;
            }
            return 0;
        }
        if (err != EAGAIN) {
            return err;
        }
        wait_hydra(context, HYDRA_WAIT_REQUEST, notify, stats);
    } while (now_ns() < deadline);
    return ETIMEDOUT;
}

static int recv_response_wait(hydra_ctx *context, uint64_t *offset,
                              const hydra_msg **message,
                              const uint8_t **payload, uint64_t timeout_ms,
                              hydra_notify_mode notify)
{
    uint64_t deadline = now_ns() + timeout_ms * UINT64_C(1000000);
    int err;

    do {
        err = hydra_recv_response(context, offset, message, payload);
        if (err == 0) {
            return 0;
        }
        if (err != EAGAIN) {
            return err;
        }
        wait_hydra(context, HYDRA_WAIT_RESPONSE, notify, NULL);
    } while (now_ns() < deadline);
    return ETIMEDOUT;
}

static int send_response_wait(hydra_ctx *context, const hydra_msg *request,
                              uint32_t status, const void *payload,
                              uint32_t length, uint64_t timeout_ms,
                              hydra_notify_mode notify)
{
    uint64_t deadline = now_ns() + timeout_ms * UINT64_C(1000000);
    int err;

    do {
        err = hydra_send_response(context, request, status, payload, length);
        if (err == 0) {
            return 0;
        }
        if (err != EAGAIN) {
            return err;
        }
        wait_hydra(context, HYDRA_WAIT_RESPONSE_SPACE, notify, NULL);
    } while (now_ns() < deadline);
    return ETIMEDOUT;
}

static int write_chunk(hydra_msg *message, void *payload, uint32_t length,
                       void *opaque)
{
    pr_write_chunk_arg *arg = opaque;
    pr_chunk_v1 *header = payload;
    double *values = (double *)((unsigned char *)payload + sizeof(*header));
    uint64_t count = arg->edge_end - arg->edge_begin;

    (void)message;
    if (length != sizeof(*header) + count * sizeof(*values)) {
        return EINVAL;
    }
    memset(header, 0, sizeof(*header));
    header->magic = PR_WIRE_MAGIC;
    header->version = PR_WIRE_VERSION;
    header->size = sizeof(*header);
    header->run_id = arg->ctx->run_id;
    header->iteration = arg->iteration;
    header->tid = arg->tid;
    header->chunk = arg->chunk;
    header->value_count = (uint32_t)count;
    for (uint64_t index = 0; index < count; index++) {
        uint64_t edge_index = arg->edge_begin + index;
        uint32_t source = arg->ctx->graph.remote_out.edges[edge_index].src;
        values[index] = arg->source[source] / arg->ctx->options.degree;
    }
    return 0;
}

static int send_chunk(pr_node_ctx *ctx, uint32_t tid, uint32_t iteration,
                      uint32_t chunk, uint64_t edge_begin, uint64_t edge_end,
                      const double *source)
{
    pr_write_chunk_arg writer = {
        .ctx = ctx,
        .tid = tid,
        .iteration = iteration,
        .chunk = chunk,
        .edge_begin = edge_begin,
        .edge_end = edge_end,
        .source = source,
    };
    pr_transport_stats *stats = &ctx->transport[tid];
    uint64_t count = edge_end - edge_begin;
    uint64_t length64 = sizeof(pr_chunk_v1) + count * sizeof(double);
    uint64_t deadline = now_ns() +
                        ctx->options.timeout_ms * UINT64_C(1000000);
    uint64_t begin = now_ns();
    int err;

    if (count > UINT32_MAX || length64 > ctx->options.payload_max ||
        length64 > UINT32_MAX) {
        return EMSGSIZE;
    }
    uint64_t req_id = ((uint64_t)iteration << 32) |
                      ((uint64_t)tid << 20) | chunk;
    do {
        err = hydra_send_request_with_writer_observed(
            ctx->tx[tid], req_id, PR_OP_DATA, (uint32_t)length64, now_ns(),
            write_chunk, &writer, NULL, NULL);
        if (err == 0) {
            stats->send_ns += now_ns() - begin;
            stats->messages_sent++;
            stats->payload_bytes_sent += length64;
            return 0;
        }
        if (err != EAGAIN) {
            return err;
        }
        stats->send_retries++;
        wait_hydra(ctx->tx[tid], HYDRA_WAIT_REQUEST_SPACE,
                   ctx->options.notify_mode, stats);
    } while (now_ns() < deadline);
    return ETIMEDOUT;
}

static int recv_chunk(pr_node_ctx *ctx, uint32_t tid, uint32_t iteration,
                      uint32_t chunk, uint64_t expected_count,
                      const double **values, uint64_t *message_offset)
{
    pr_transport_stats *stats = &ctx->transport[tid];
    const hydra_msg *message;
    const uint8_t *payload;
    int err = recv_request_wait(ctx->rx[tid], message_offset, &message,
                                &payload, ctx->options.timeout_ms,
                                &stats->recv_wait_ns,
                                ctx->options.notify_mode, stats);
    if (err != 0) {
        return err;
    }
    if (message->op != PR_OP_DATA || message->len < sizeof(pr_chunk_v1)) {
        return EPROTO;
    }
    const pr_chunk_v1 *header = (const pr_chunk_v1 *)payload;
    if (header->magic != PR_WIRE_MAGIC ||
        header->version != PR_WIRE_VERSION ||
        header->size != sizeof(*header) || header->run_id != ctx->run_id ||
        header->iteration != iteration || header->tid != tid ||
        header->chunk != chunk || header->value_count != expected_count ||
        message->len != sizeof(*header) + expected_count * sizeof(double)) {
        return EILSEQ;
    }
    *values = (const double *)(payload + sizeof(*header));
    stats->messages_received++;
    stats->payload_bytes_received += message->len;
    return 0;
}

static int send_barrier(pr_node_ctx *ctx, uint32_t tid, uint32_t iteration)
{
    pr_chunk_v1 header = {
        .magic = PR_WIRE_MAGIC,
        .version = PR_WIRE_VERSION,
        .size = sizeof(header),
        .run_id = ctx->run_id,
        .iteration = iteration,
        .tid = tid,
        .chunk = ctx->options.chunks,
        .value_count = 0,
    };
    pr_transport_stats *stats = &ctx->transport[tid];
    uint64_t begin = now_ns();
    uint64_t req_id = ((uint64_t)iteration << 32) |
                      ((uint64_t)tid << 20) | UINT64_C(0xfffff);
    int err = send_request_wait(ctx->tx[tid], req_id, PR_OP_BARRIER,
                                &header, sizeof(header),
                                ctx->options.timeout_ms,
                                ctx->options.notify_mode, stats);
    stats->send_ns += now_ns() - begin;
    if (err == 0) {
        stats->messages_sent++;
        stats->payload_bytes_sent += sizeof(header);
    }
    return err;
}

static int recv_barrier(pr_node_ctx *ctx, uint32_t tid, uint32_t iteration)
{
    pr_transport_stats *stats = &ctx->transport[tid];
    const hydra_msg *message;
    const uint8_t *payload;
    uint64_t offset;
    int err = recv_request_wait(ctx->rx[tid], &offset, &message, &payload,
                                ctx->options.timeout_ms,
                                &stats->recv_wait_ns,
                                ctx->options.notify_mode, stats);
    if (err != 0) {
        return err;
    }
    const pr_chunk_v1 *header = (const pr_chunk_v1 *)payload;
    if (message->op != PR_OP_BARRIER || message->len != sizeof(*header) ||
        header->magic != PR_WIRE_MAGIC ||
        header->version != PR_WIRE_VERSION ||
        header->run_id != ctx->run_id || header->iteration != iteration ||
        header->tid != tid || header->chunk != ctx->options.chunks ||
        header->value_count != 0) {
        return EILSEQ;
    }
    stats->messages_received++;
    stats->payload_bytes_received += message->len;
    uint64_t release_begin = now_ns();
    err = hydra_release_message(ctx->rx[tid], offset);
    stats->release_ns += now_ns() - release_begin;
    return err;
}

static void remember_worker_error(pr_node_ctx *ctx, int err)
{
    int expected = 0;

    (void)atomic_compare_exchange_strong(&ctx->worker_error, &expected, err);
}

static void *worker_main(void *opaque)
{
    pr_worker_arg *argument = opaque;
    pr_node_ctx *ctx = argument->ctx;
    uint32_t tid = argument->tid;
    int cpus[PR_MAX_THREADS];
    int cpu_count = parse_cpu_list(ctx->options.cpu_list, cpus,
                                   PR_MAX_THREADS);
    uint64_t vertex_begin = ctx->options.vertices * tid /
                            ctx->options.threads;
    uint64_t vertex_end = ctx->options.vertices * (tid + 1) /
                          ctx->options.threads;
    double initial_rank = 1.0 / (2.0 * ctx->options.vertices);
    double base_rank = 0.15 / (2.0 * ctx->options.vertices);
    const double damping = 0.85;
    uint64_t start_ns = 0;
    int err = 0;

    if (cpu_count != (int)ctx->options.threads ||
        (err = pin_thread(cpus[tid])) != 0) {
        remember_worker_error(ctx, err != 0 ? err : EINVAL);
    }
    for (uint64_t vertex = vertex_begin; vertex < vertex_end; vertex++) {
        ctx->rank[0][vertex] = initial_rank;
        ctx->rank[1][vertex] = 0.0;
    }
    pthread_barrier_wait(&ctx->barrier);
    while (!atomic_load_explicit(&ctx->start_gate, memory_order_acquire)) {
        hydra_cpu_relax();
    }
    pthread_barrier_wait(&ctx->barrier);
    if (tid == 0) {
        start_ns = now_ns();
    }
    pthread_barrier_wait(&ctx->barrier);

    for (uint32_t iteration = 0; iteration < ctx->options.iterations;
         iteration++) {
        double *source = ctx->rank[iteration & 1U];
        double *target = ctx->rank[(iteration + 1U) & 1U];
        double local_residual = 0.0;

        for (uint32_t chunk = 0; chunk < ctx->options.chunks; chunk++) {
            uint64_t chunk_begin = vertex_begin +
                (vertex_end - vertex_begin) * chunk / ctx->options.chunks;
            uint64_t chunk_end = vertex_begin +
                (vertex_end - vertex_begin) * (chunk + 1) /
                ctx->options.chunks;
            uint64_t send_begin =
                ctx->graph.remote_out.offsets[chunk_begin];
            uint64_t send_end = ctx->graph.remote_out.offsets[chunk_end];
            uint64_t recv_begin =
                ctx->graph.remote_in.offsets[chunk_begin];
            uint64_t recv_end = ctx->graph.remote_in.offsets[chunk_end];
            const double *remote_values = NULL;
            uint64_t message_offset = 0;

            if (atomic_load(&ctx->worker_error) == 0) {
                err = send_chunk(ctx, tid, iteration, chunk, send_begin,
                                 send_end, source);
                if (err == 0) {
                    err = recv_chunk(ctx, tid, iteration, chunk,
                                     recv_end - recv_begin, &remote_values,
                                     &message_offset);
                }
                if (err != 0) {
                    remember_worker_error(ctx, err);
                }
            }
            if (err == 0) {
                for (uint64_t vertex = chunk_begin; vertex < chunk_end;
                     vertex++) {
                    double incoming = 0.0;
                    for (uint64_t edge =
                             ctx->graph.local_in.offsets[vertex];
                         edge < ctx->graph.local_in.offsets[vertex + 1];
                         edge++) {
                        incoming += source[ctx->graph.local_in.edges[edge].src] /
                                    ctx->options.degree;
                    }
                    for (uint64_t edge =
                             ctx->graph.remote_in.offsets[vertex];
                         edge < ctx->graph.remote_in.offsets[vertex + 1];
                         edge++) {
                        incoming += remote_values[edge - recv_begin];
                    }
                    target[vertex] = base_rank + damping * incoming;
                    local_residual += fabs(target[vertex] - source[vertex]);
                }
                uint64_t release_begin = now_ns();
                err = hydra_release_message(ctx->rx[tid], message_offset);
                ctx->transport[tid].release_ns += now_ns() - release_begin;
                if (err != 0) {
                    remember_worker_error(ctx, err);
                }
            }
        }
        ctx->partials[tid] = local_residual;
        pthread_barrier_wait(&ctx->barrier);
        if (tid == 0) {
            double residual = 0.0;
            for (uint32_t worker = 0; worker < ctx->options.threads;
                 worker++) {
                residual += ctx->partials[worker];
            }
            ctx->residual = residual;
        }
        pthread_barrier_wait(&ctx->barrier);
        if (atomic_load(&ctx->worker_error) == 0) {
            err = send_barrier(ctx, tid, iteration);
            if (err == 0) {
                err = recv_barrier(ctx, tid, iteration);
            }
            if (err != 0) {
                remember_worker_error(ctx, err);
            }
        }
        pthread_barrier_wait(&ctx->barrier);
    }

    double *final_rank = ctx->rank[ctx->options.iterations & 1U];
    double checksum = 0.0;
    for (uint64_t vertex = vertex_begin; vertex < vertex_end; vertex++) {
        checksum += final_rank[vertex];
    }
    ctx->partials[tid] = checksum;
    pthread_barrier_wait(&ctx->barrier);
    if (tid == 0) {
        uint64_t end_ns = now_ns();
        double total = 0.0;
        for (uint32_t worker = 0; worker < ctx->options.threads; worker++) {
            total += ctx->partials[worker];
        }
        ctx->elapsed_ns = end_ns - start_ns;
        ctx->rank_checksum = total;
    }
    return NULL;
}

static void *alloc_rank(uint64_t bytes)
{
    void *mapping = mmap(NULL, (size_t)bytes, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        return NULL;
    }
    (void)madvise(mapping, (size_t)bytes, MADV_HUGEPAGE);
    return mapping;
}

static int node_init_compute(pr_node_ctx *ctx)
{
    uint64_t rank_bytes = ctx->options.vertices * sizeof(double);

    ctx->rank[0] = alloc_rank(rank_bytes);
    ctx->rank[1] = alloc_rank(rank_bytes);
    ctx->partials = calloc(ctx->options.threads, sizeof(*ctx->partials));
    ctx->transport = calloc(ctx->options.threads, sizeof(*ctx->transport));
    if (ctx->rank[0] == NULL || ctx->rank[1] == NULL ||
        ctx->partials == NULL || ctx->transport == NULL) {
        return ENOMEM;
    }
    int err = pthread_barrier_init(&ctx->barrier, NULL,
                                   ctx->options.threads);
    if (err != 0) {
        return err;
    }
    atomic_init(&ctx->start_gate, false);
    atomic_init(&ctx->worker_error, 0);
    for (uint32_t tid = 0; tid < ctx->options.threads; tid++) {
        ctx->worker_arg[tid] = (pr_worker_arg) {.ctx = ctx, .tid = tid};
        err = pthread_create(&ctx->worker[tid], NULL, worker_main,
                             &ctx->worker_arg[tid]);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int node_join_compute(pr_node_ctx *ctx)
{
    int join_error = 0;
    for (uint32_t tid = 0; tid < ctx->options.threads; tid++) {
        int err = pthread_join(ctx->worker[tid], NULL);
        if (err != 0 && join_error == 0) {
            join_error = err;
        }
    }
    int worker_error = atomic_load(&ctx->worker_error);
    return worker_error != 0 ? worker_error : join_error;
}

static void sum_transport(const pr_node_ctx *ctx, pr_node_result_v1 *result)
{
    for (uint32_t tid = 0; tid < ctx->options.threads; tid++) {
        const pr_transport_stats *stats = &ctx->transport[tid];
        result->hydra_send_ns += stats->send_ns;
        result->hydra_recv_wait_ns += stats->recv_wait_ns;
        result->hydra_release_ns += stats->release_ns;
        result->messages_sent += stats->messages_sent;
        result->messages_received += stats->messages_received;
        result->payload_bytes_sent += stats->payload_bytes_sent;
        result->payload_bytes_received += stats->payload_bytes_received;
        result->send_retries += stats->send_retries;
        result->hydra_wait_ns += stats->wait_ns;
        result->hydra_wait_calls += stats->wait_calls;
    }
}

static void fill_node_result(const pr_node_ctx *ctx, pr_node_result_v1 *result)
{
    memset(result, 0, sizeof(*result));
    result->magic = PR_WIRE_MAGIC;
    result->version = PR_WIRE_VERSION;
    result->size = sizeof(*result);
    result->run_id = ctx->run_id;
    result->elapsed_ns = ctx->elapsed_ns;
    result->graph_checksum = ctx->graph_checksum;
    result->residual = ctx->residual;
    result->rank_checksum = ctx->rank_checksum;
    sum_transport(ctx, result);
}

static bool valid_node_result(const pr_node_result_v1 *result,
                              uint64_t run_id)
{
    return result->magic == PR_WIRE_MAGIC &&
           result->version == PR_WIRE_VERSION &&
           result->size == sizeof(*result) && result->run_id == run_id;
}

static int node_open_data_contexts(pr_node_ctx *ctx, bool create)
{
    int err;

    for (uint32_t tid = 0; tid < ctx->options.threads; tid++) {
        uint64_t connection = tid + 1;
        if (create) {
            err = open_hydra(&ctx->tx[tid], &ctx->options,
                             ctx->options.tx_path, connection, false);
            if (err == 0) {
                err = open_hydra(&ctx->rx[tid], &ctx->options,
                                 ctx->options.rx_path, connection, false);
            }
        } else {
            err = open_hydra_retry(&ctx->tx[tid], &ctx->options,
                                   ctx->options.tx_path, connection);
            if (err == 0) {
                err = open_hydra_retry(&ctx->rx[tid], &ctx->options,
                                       ctx->options.rx_path, connection);
            }
        }
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static void node_close(pr_node_ctx *ctx)
{
    uint64_t rank_bytes = ctx->options.vertices * sizeof(double);

    for (uint32_t tid = 0; tid < ctx->options.threads; tid++) {
        hydra_close(ctx->tx[tid]);
        hydra_close(ctx->rx[tid]);
    }
    if (ctx->rank[0] != NULL) {
        munmap(ctx->rank[0], (size_t)rank_bytes);
    }
    if (ctx->rank[1] != NULL) {
        munmap(ctx->rank[1], (size_t)rank_bytes);
    }
    free(ctx->partials);
    free(ctx->transport);
    pr_graph_destroy(&ctx->graph);
}

static int release_control_message(hydra_ctx *context, uint64_t offset,
                                   int prior_error)
{
    int release_error = hydra_release_message(context, offset);
    return prior_error != 0 ? prior_error : release_error;
}

static int run_coordinator(const pr_options *options)
{
    pr_node_ctx ctx = {.node = 0, .options = *options};
    hydra_ctx *control = NULL;
    hydra_ctx *reverse_control = NULL;
    const hydra_msg *message;
    const uint8_t *payload;
    uint64_t offset;
    int err;
    uint64_t build_begin = now_ns();

    ctx.run_id = now_ns() ^ (uint64_t)getpid();
    err = open_hydra(&control, options, options->tx_path, 0, true);
    if (err == 0) {
        err = open_hydra(&reverse_control, options, options->rx_path, 0,
                         true);
    }
    if (err != 0) {
        print_error("create HydraRPC endpoints", err);
        return 1;
    }
    err = node_open_data_contexts(&ctx, true);
    if (err == 0) {
        err = pr_graph_build_local(&ctx.graph, 0, options->vertices,
                                   options->degree,
                                   options->remote_degree);
    }
    if (err == 0) {
        ctx.graph_checksum = pr_graph_checksum(&ctx.graph);
    }
    uint64_t graph_build_ns = now_ns() - build_begin;
    if (err != 0) {
        print_error("coordinator setup", err);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }

    pr_arm_v1 arm = {
        .magic = PR_WIRE_MAGIC,
        .version = PR_WIRE_VERSION,
        .size = sizeof(arm),
        .run_id = ctx.run_id,
        .vertices = options->vertices,
        .degree = options->degree,
        .remote_degree = options->remote_degree,
        .iterations = options->iterations,
        .chunks = options->chunks,
        .threads = options->threads,
        .state_mode = options->state_mode,
        .key = options->cxlfork.key,
        .generation = options->cxlfork.generation,
        .page_checksum = options->cxlfork.page_checksum,
        .template_base = options->cxlfork.base,
        .template_bytes = options->cxlfork.bytes,
    };
    uint64_t job_begin_ns = now_ns();
    err = send_request_wait(control, ctx.run_id, PR_OP_ARM, &arm,
                            sizeof(arm), options->timeout_ms,
                            options->notify_mode, NULL);
    if (err == 0) {
        err = recv_response_wait(control, &offset, &message, &payload,
                                 options->timeout_ms, options->notify_mode);
    }
    pr_arm_result_v1 remote_arm;
    if (err == 0 && (message->status != 0 ||
                     message->len != sizeof(remote_arm))) {
        err = message->status != 0 ? (int)message->status : EPROTO;
    }
    if (err == 0) {
        memcpy(&remote_arm, payload, sizeof(remote_arm));
        if (remote_arm.magic != PR_WIRE_MAGIC ||
            remote_arm.version != PR_WIRE_VERSION ||
            remote_arm.size != sizeof(remote_arm) ||
            remote_arm.run_id != ctx.run_id) {
            err = EILSEQ;
        }
        err = release_control_message(control, offset, err);
    }
    if (err != 0) {
        print_error("ARM worker", err);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }

    uint64_t local_thread_begin = now_ns();
    err = node_init_compute(&ctx);
    uint64_t local_thread_create_ns = now_ns() - local_thread_begin;
    if (err != 0) {
        print_error("create coordinator threads", err);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }
    pr_start_v1 start = {
        .magic = PR_WIRE_MAGIC,
        .version = PR_WIRE_VERSION,
        .size = sizeof(start),
        .run_id = ctx.run_id,
    };
    uint64_t start_publish_begin = now_ns();
    err = send_request_wait(control, ctx.run_id + 1, PR_OP_START, &start,
                            sizeof(start), options->timeout_ms,
                            options->notify_mode, NULL);
    uint64_t start_publish_ns = now_ns() - start_publish_begin;
    atomic_store_explicit(&ctx.start_gate, true, memory_order_release);
    int local_join_error = node_join_compute(&ctx);
    if (err == 0) {
        err = local_join_error;
    }
    if (err == 0) {
        err = recv_response_wait(control, &offset, &message, &payload,
                                 options->timeout_ms, options->notify_mode);
    }
    pr_node_result_v1 remote_result;
    if (err == 0 && (message->status != 0 ||
                     message->len != sizeof(remote_result))) {
        err = message->status != 0 ? (int)message->status : EPROTO;
    }
    if (err == 0) {
        memcpy(&remote_result, payload, sizeof(remote_result));
        if (!valid_node_result(&remote_result, ctx.run_id)) {
            err = EILSEQ;
        }
        err = release_control_message(control, offset, err);
    }
    uint64_t job_end_ns = now_ns();
    if (err != 0) {
        print_error("execute distributed PageRank", err);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }

    pr_node_result_v1 local_result;
    fill_node_result(&ctx, &local_result);
    double rank_checksum = local_result.rank_checksum +
                           remote_result.rank_checksum;
    double residual = local_result.residual + remote_result.residual;
    uint64_t elapsed_ns = local_result.elapsed_ns > remote_result.elapsed_ns
                            ? local_result.elapsed_ns
                            : remote_result.elapsed_ns;
    uint64_t messages = local_result.messages_sent +
                        remote_result.messages_sent;
    uint64_t bytes = local_result.payload_bytes_sent +
                     remote_result.payload_bytes_sent;
    uint64_t hydra_send_ns = local_result.hydra_send_ns +
                             remote_result.hydra_send_ns;
    uint64_t hydra_recv_wait_ns = local_result.hydra_recv_wait_ns +
                                  remote_result.hydra_recv_wait_ns;
    uint64_t hydra_release_ns = local_result.hydra_release_ns +
                                remote_result.hydra_release_ns;
    uint64_t hydra_wait_ns = local_result.hydra_wait_ns +
                             remote_result.hydra_wait_ns;
    uint64_t hydra_wait_calls = local_result.hydra_wait_calls +
                                remote_result.hydra_wait_calls;
    double edges = 2.0 * options->vertices * options->degree *
                   options->iterations;

    printf("pr_run_v1 status=ok backend=%s state=%s notify=%s"
           " effective_notify=%s run_id=%" PRIu64
           " vertices=%" PRIu64 " degree=%u remote_degree=%u"
           " iterations=%u chunks=%u threads_per_node=%u total_threads=%u"
           " elapsed_ms=%.3f medges_per_sec=%.3f job_e2e_ms=%.3f"
           " graph_build_ms=%.3f arm_ms=%.3f"
           " local_thread_create_ms=%.3f start_publish_ms=%.3f"
           " restore_wrapper_ms=%.3f restore_ioctl_ms=%.3f"
           " restore_kernel_ms=%.3f restore_leaf_attach_ms=%.3f"
           " restore_fallback_ms=%.3f shared_leaf_count=%" PRIu64
           " shared_data_pages=%" PRIu64 " fallback_pages=%" PRIu64
           " hydra_send_cpu_sum_ms=%.3f hydra_recv_wait_sum_ms=%.3f"
           " hydra_release_sum_ms=%.3f hydra_wait_cpu_sum_ms=%.3f"
           " hydra_wait_calls=%" PRIu64 " messages=%" PRIu64
           " payload_bytes=%" PRIu64 " residual=%.12g checksum=%.12g"
           " node0_graph_checksum=%" PRIu64
           " node1_graph_checksum=%" PRIu64 " verify=ok\n",
           backend_name(options->backend), state_name(options->state_mode),
           hydra_notify_mode_name(options->notify_mode),
           hydra_notify_mode_name(
               hydra_effective_notify_mode(options->notify_mode)),
           ctx.run_id, 2 * options->vertices, options->degree,
           options->remote_degree, options->iterations, options->chunks,
           options->threads, options->threads * 2,
           elapsed_ns / 1e6, edges / (elapsed_ns / 1e9) / 1e6,
           (job_end_ns - job_begin_ns) / 1e6, graph_build_ns / 1e6,
           remote_arm.arm_total_ns / 1e6,
           local_thread_create_ns / 1e6, start_publish_ns / 1e6,
           remote_arm.restore_wrapper_ns / 1e6,
           remote_arm.restore_ioctl_ns / 1e6,
           remote_arm.restore_kernel_ns / 1e6,
           remote_arm.restore_leaf_attach_ns / 1e6,
           remote_arm.restore_fallback_ns / 1e6,
           remote_arm.shared_leaf_count, remote_arm.shared_data_pages,
           remote_arm.fallback_pages, hydra_send_ns / 1e6,
           hydra_recv_wait_ns / 1e6, hydra_release_ns / 1e6,
           hydra_wait_ns / 1e6, hydra_wait_calls,
           messages, bytes, residual, rank_checksum,
           local_result.graph_checksum, remote_result.graph_checksum);

    hydra_close(control);
    hydra_close(reverse_control);
    node_close(&ctx);
    return 0;
}

static int validate_arm(const pr_options *options, const hydra_msg *message,
                        const uint8_t *payload, pr_arm_v1 *arm)
{
    if (message->op != PR_OP_ARM || message->len != sizeof(*arm)) {
        return EPROTO;
    }
    memcpy(arm, payload, sizeof(*arm));
    if (arm->magic != PR_WIRE_MAGIC || arm->version != PR_WIRE_VERSION ||
        arm->size != sizeof(*arm) || arm->vertices != options->vertices ||
        arm->degree != options->degree ||
        arm->remote_degree != options->remote_degree ||
        arm->iterations != options->iterations ||
        arm->chunks != options->chunks || arm->threads != options->threads ||
        arm->state_mode != (uint32_t)options->state_mode) {
        return EILSEQ;
    }
    if (options->state_mode == PR_STATE_CXLFORK &&
        (arm->key != options->cxlfork.key ||
         arm->generation != options->cxlfork.generation ||
         arm->page_checksum != options->cxlfork.page_checksum ||
         arm->template_base != options->cxlfork.base ||
         arm->template_bytes != options->cxlfork.bytes)) {
        return EILSEQ;
    }
    return 0;
}

static int run_worker(const pr_options *options)
{
    pr_node_ctx ctx = {.node = 1, .options = *options};
    pr_cxlfork_handle cxlfork = {0};
    hydra_ctx *control = NULL;
    hydra_ctx *reverse_control = NULL;
    const hydra_msg *message;
    const uint8_t *payload;
    uint64_t offset;
    int err;

    err = open_hydra_retry(&control, options, options->rx_path, 0);
    if (err == 0) {
        err = open_hydra_retry(&reverse_control, options, options->tx_path, 0);
    }
    if (err == 0) {
        err = node_open_data_contexts(&ctx, false);
    }
    if (err == 0 && options->state_mode == PR_STATE_CXLFORK) {
        err = pr_cxlfork_context_open(&options->cxlfork, &cxlfork);
    }
    if (err != 0) {
        print_error("worker preopen", err);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }

    err = recv_request_wait(control, &offset, &message, &payload,
                            options->timeout_ms, NULL,
                            options->notify_mode, NULL);
    pr_arm_v1 arm;
    if (err == 0) {
        err = validate_arm(options, message, payload, &arm);
    }
    uint64_t arm_begin_ns = now_ns();
    uint64_t graph_begin_ns = arm_begin_ns;
    if (err == 0) {
        ctx.run_id = arm.run_id;
        if (options->state_mode == PR_STATE_CXLFORK) {
            err = pr_cxlfork_restore(&options->cxlfork, &cxlfork,
                                     &ctx.graph);
            if (err == 0) {
                ctx.graph_checksum = cxlfork.graph_checksum;
                err = pr_cxlfork_collect(&cxlfork);
            }
        } else {
            err = pr_graph_build_local(&ctx.graph, 1, options->vertices,
                                       options->degree,
                                       options->remote_degree);
            if (err == 0) {
                ctx.graph_checksum = pr_graph_checksum(&ctx.graph);
            }
        }
    }
    uint64_t graph_ready_ns = now_ns() - graph_begin_ns;
    uint64_t thread_begin_ns = now_ns();
    if (err == 0) {
        err = node_init_compute(&ctx);
    }
    uint64_t thread_create_ns = now_ns() - thread_begin_ns;
    pr_arm_result_v1 arm_result = {
        .magic = PR_WIRE_MAGIC,
        .version = PR_WIRE_VERSION,
        .size = sizeof(arm_result),
        .run_id = ctx.run_id,
        .arm_total_ns = now_ns() - arm_begin_ns,
        .graph_ready_ns = graph_ready_ns,
        .thread_create_ns = thread_create_ns,
        .graph_checksum = err == 0 ? ctx.graph_checksum : 0,
        .restore_wrapper_ns = cxlfork.timing.wrapper_total_ns,
        .restore_ioctl_ns = cxlfork.timing.resume_ioctl_ns,
        .restore_kernel_ns = cxlfork.result.kernel_total_ns,
        .restore_lifecycle_ns = cxlfork.result.lifecycle_ns,
        .restore_validation_ns = cxlfork.result.validation_ns,
        .restore_vma_ns = cxlfork.result.vma_create_ns,
        .restore_upper_pt_ns = cxlfork.result.upper_pt_alloc_ns,
        .restore_leaf_attach_ns =
            cxlfork.result.shared_leaf_attach_exclusive_ns != 0
                ? cxlfork.result.shared_leaf_attach_exclusive_ns
                : cxlfork.result.shared_leaf_attach_ns,
        .restore_fallback_ns = cxlfork.result.fallback_reconstruct_ns +
                               cxlfork.result.fallback_other_ns,
        .shared_leaf_count = cxlfork.result.shared_leaf_count,
        .shared_data_pages = cxlfork.result.shared_data_pages,
        .fallback_pages = cxlfork.result.fallback_pages,
    };
    int response_error = send_response_wait(
        control, message, err == 0 ? 0U : (uint32_t)err,
        &arm_result, sizeof(arm_result), options->timeout_ms,
        options->notify_mode);
    err = release_control_message(control, offset,
                                  err != 0 ? err : response_error);
    if (err != 0) {
        print_error("worker ARM", err);
        pr_cxlfork_close(&cxlfork);
        hydra_close(control);
        hydra_close(reverse_control);
        node_close(&ctx);
        return 1;
    }

    err = recv_request_wait(control, &offset, &message, &payload,
                            options->timeout_ms, NULL,
                            options->notify_mode, NULL);
    bool have_start_request = err == 0;
    if (err == 0) {
        if (message->op != PR_OP_START ||
            message->len != sizeof(pr_start_v1)) {
            err = EPROTO;
        } else {
            const pr_start_v1 *start = (const pr_start_v1 *)payload;
            if (start->magic != PR_WIRE_MAGIC ||
                start->version != PR_WIRE_VERSION ||
                start->size != sizeof(*start) ||
                start->run_id != ctx.run_id) {
                err = EILSEQ;
            }
        }
    }
    hydra_msg start_request;
    if (have_start_request) {
        start_request = *message;
    } else {
        memset(&start_request, 0, sizeof(start_request));
    }
    atomic_store_explicit(&ctx.start_gate, true, memory_order_release);
    int join_error = node_join_compute(&ctx);
    if (err == 0) {
        err = join_error;
    }
    if (options->state_mode == PR_STATE_CXLFORK && err == 0) {
        int collect_error = pr_cxlfork_collect(&cxlfork);
        if (collect_error != 0) {
            err = collect_error;
        }
    }
    pr_node_result_v1 result;
    fill_node_result(&ctx, &result);
    if (have_start_request) {
        response_error = send_response_wait(
            control, &start_request, err == 0 ? 0U : (uint32_t)err,
            &result, sizeof(result), options->timeout_ms,
            options->notify_mode);
        err = release_control_message(control, offset,
                                      err != 0 ? err : response_error);
    }
    if (err == 0) {
        printf("pr_worker_v1 status=ok state=%s run_id=%" PRIu64
               " elapsed_ms=%.3f restore_wrapper_ms=%.3f"
               " graph_checksum=%" PRIu64 " checksum=%.12g verify=ok\n",
               state_name(options->state_mode), ctx.run_id,
               ctx.elapsed_ns / 1e6, cxlfork.timing.wrapper_total_ns / 1e6,
               result.graph_checksum, result.rank_checksum);
    } else {
        print_error("worker execution", err);
    }
    pr_cxlfork_close(&cxlfork);
    hydra_close(control);
    hydra_close(reverse_control);
    node_close(&ctx);
    return err == 0 ? 0 : 1;
}

static int run_template(const pr_options *options)
{
    int err = pr_cxlfork_publish(&options->cxlfork);
    if (err != 0) {
        print_error("publish PageRank CXLfork template", err);
        return 1;
    }
    return 0;
}

static int run_graph_check(const pr_options *options)
{
    pr_graph graphs[2] = {0};
    pr_graph template_graph = {0};
    pr_graph rebound_graph = {0};
    int err = pr_graph_build_local(&graphs[0], 0, options->vertices,
                                   options->degree,
                                   options->remote_degree);
    if (err == 0) {
        err = pr_graph_build_local(&graphs[1], 1, options->vertices,
                                   options->degree,
                                   options->remote_degree);
    }
    if (err != 0) {
        print_error("build graph", err);
        pr_graph_destroy(&graphs[0]);
        pr_graph_destroy(&graphs[1]);
        return 1;
    }
    bool cross_match =
        graphs[0].remote_out.count == graphs[1].remote_in.count &&
        memcmp(graphs[0].remote_out.edges, graphs[1].remote_in.edges,
               (size_t)graphs[0].remote_out.edge_bytes) == 0 &&
        memcmp(graphs[0].remote_out.offsets, graphs[1].remote_in.offsets,
               (size_t)graphs[0].remote_out.offset_bytes) == 0 &&
        graphs[1].remote_out.count == graphs[0].remote_in.count &&
        memcmp(graphs[1].remote_out.edges, graphs[0].remote_in.edges,
               (size_t)graphs[1].remote_out.edge_bytes) == 0 &&
        memcmp(graphs[1].remote_out.offsets, graphs[0].remote_in.offsets,
               (size_t)graphs[1].remote_out.offset_bytes) == 0;
    void *template_mapping = mmap(NULL, (size_t)options->cxlfork.bytes,
                                  PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    bool template_match = false;
    if (template_mapping != MAP_FAILED &&
        pr_graph_build_template(template_mapping, options->cxlfork.bytes, 1,
                                options->vertices, options->degree,
                                options->remote_degree,
                                &template_graph) == 0 &&
        pr_graph_bind_template(template_mapping, options->cxlfork.bytes, 1,
                               options->vertices, options->degree,
                               options->remote_degree, true,
                               &rebound_graph) == 0) {
        template_match = pr_graph_checksum(&graphs[1]) ==
                         pr_graph_checksum(&rebound_graph);
    }
    printf("pr_graph_check_v1 status=%s vertices=%" PRIu64
           " degree=%u remote_degree=%u node0_checksum=%" PRIu64
           " node1_checksum=%" PRIu64
           " cross_match=%s template_match=%s verify=%s\n",
           cross_match && template_match ? "ok" : "error",
           options->vertices,
           options->degree, options->remote_degree,
           pr_graph_checksum(&graphs[0]), pr_graph_checksum(&graphs[1]),
           cross_match ? "yes" : "no", template_match ? "yes" : "no",
           cross_match && template_match ? "ok" : "fail");
    if (template_mapping != MAP_FAILED) {
        munmap(template_mapping, (size_t)options->cxlfork.bytes);
    }
    pr_graph_destroy(&graphs[0]);
    pr_graph_destroy(&graphs[1]);
    return cross_match && template_match ? 0 : 1;
}

static void usage(const char *program)
{
    fprintf(stderr,
            "usage: %s graph-check|template|inspect|release|coordinator|worker [options]\n"
            "  --backend=file|ivshmem|devdax --tx-path=PATH --rx-path=PATH\n"
            "  --vertices-per-node=N --degree=N --remote-degree=N\n"
            "  --iterations=N --chunks=N --threads=N --cpus=LIST\n"
            "  --state=local|cxlfork --checksum=on|off --timeout-ms=N\n"
            "  --notify=umwait|busy\n"
            "  --map-size=N --queue-capacity=N --payload-max=N\n"
            "  --key=N --generation=N --page-checksum=N\n"
            "  --template-base=N --template-bytes=N --full-validate\n",
            program);
}

static void default_options(pr_options *options)
{
    memset(options, 0, sizeof(*options));
    options->backend = PR_BACKEND_FILE;
    options->state_mode = PR_STATE_LOCAL;
    options->map_size = PR_DEFAULT_MAP_SIZE;
    options->queue_capacity = PR_DEFAULT_QUEUE_CAPACITY;
    options->payload_max = PR_DEFAULT_PAYLOAD_MAX;
    options->vertices = 262144;
    options->degree = 16;
    options->remote_degree = 2;
    options->iterations = 100;
    options->chunks = 32;
    options->threads = 16;
    options->timeout_ms = 120000;
    options->notify_mode = HYDRA_NOTIFY_UMWAIT;
    options->cxlfork.base = PR_TEMPLATE_BASE_DEFAULT;
    options->cxlfork.bytes = PR_TEMPLATE_BYTES_DEFAULT;
}

static int parse_options(int argc, char **argv, pr_options *options)
{
    const char *value;

    default_options(options);
    for (int index = 2; index < argc; index++) {
        const char *arg = argv[index];
        if (option_value(arg, "--backend=", &value)) {
            if (strcmp(value, "file") == 0) {
                options->backend = PR_BACKEND_FILE;
            } else if (strcmp(value, "ivshmem") == 0) {
                options->backend = PR_BACKEND_IVSHMEM;
            } else if (strcmp(value, "devdax") == 0) {
                options->backend = PR_BACKEND_DEVDAX;
            } else {
                return EINVAL;
            }
        } else if (option_value(arg, "--state=", &value)) {
            if (strcmp(value, "local") == 0) {
                options->state_mode = PR_STATE_LOCAL;
            } else if (strcmp(value, "cxlfork") == 0) {
                options->state_mode = PR_STATE_CXLFORK;
            } else {
                return EINVAL;
            }
        } else if (option_value(arg, "--tx-path=", &value)) {
            options->tx_path = value;
        } else if (option_value(arg, "--rx-path=", &value)) {
            options->rx_path = value;
        } else if (option_value(arg, "--cpus=", &value)) {
            options->cpu_list = value;
        } else if (option_value(arg, "--map-size=", &value)) {
            options->map_size = parse_u64(value, "map size");
        } else if (option_value(arg, "--queue-capacity=", &value)) {
            options->queue_capacity = parse_u64(value, "queue capacity");
        } else if (option_value(arg, "--payload-max=", &value)) {
            options->payload_max = parse_u64(value, "payload max");
        } else if (option_value(arg, "--vertices-per-node=", &value)) {
            options->vertices = parse_u64(value, "vertices per node");
        } else if (option_value(arg, "--degree=", &value)) {
            options->degree = (uint32_t)parse_u64(value, "degree");
        } else if (option_value(arg, "--remote-degree=", &value)) {
            options->remote_degree =
                (uint32_t)parse_u64(value, "remote degree");
        } else if (option_value(arg, "--iterations=", &value)) {
            options->iterations = (uint32_t)parse_u64(value, "iterations");
        } else if (option_value(arg, "--chunks=", &value)) {
            options->chunks = (uint32_t)parse_u64(value, "chunks");
        } else if (option_value(arg, "--threads=", &value)) {
            options->threads = (uint32_t)parse_u64(value, "threads");
        } else if (option_value(arg, "--timeout-ms=", &value)) {
            options->timeout_ms = parse_u64(value, "timeout");
        } else if (option_value(arg, "--notify=", &value)) {
            if (strcmp(value, "umwait") == 0) {
                options->notify_mode = HYDRA_NOTIFY_UMWAIT;
            } else if (strcmp(value, "busy") == 0) {
                options->notify_mode = HYDRA_NOTIFY_BUSY;
            } else {
                return EINVAL;
            }
        } else if (option_value(arg, "--checksum=", &value)) {
            if (strcmp(value, "on") == 0) {
                options->checksum = true;
            } else if (strcmp(value, "off") == 0) {
                options->checksum = false;
            } else {
                return EINVAL;
            }
        } else if (option_value(arg, "--key=", &value)) {
            options->cxlfork.key = parse_u64(value, "CXLfork key");
        } else if (option_value(arg, "--generation=", &value)) {
            options->cxlfork.generation =
                parse_u64(value, "CXLfork generation");
        } else if (option_value(arg, "--page-checksum=", &value)) {
            options->cxlfork.page_checksum =
                parse_u64(value, "CXLfork page checksum");
        } else if (option_value(arg, "--template-base=", &value)) {
            options->cxlfork.base = parse_u64(value, "template base");
        } else if (option_value(arg, "--template-bytes=", &value)) {
            options->cxlfork.bytes = parse_u64(value, "template bytes");
        } else if (strcmp(arg, "--full-validate") == 0) {
            options->cxlfork.full_validate = true;
        } else {
            fprintf(stderr, "unknown option: %s\n", arg);
            return EINVAL;
        }
    }
    options->cxlfork.vertices = options->vertices;
    options->cxlfork.degree = options->degree;
    options->cxlfork.remote_degree = options->remote_degree;
    if (options->vertices < 1024 || options->vertices > UINT32_MAX ||
        options->degree < 2 || options->remote_degree == 0 ||
        options->remote_degree >= options->degree ||
        options->iterations == 0 || options->chunks == 0 ||
        options->threads == 0 || options->threads > PR_MAX_THREADS ||
        options->payload_max < sizeof(pr_chunk_v1) + sizeof(double) ||
        options->timeout_ms == 0) {
        return EINVAL;
    }
    return 0;
}

int main(int argc, char **argv)
{
    pr_options options;
    int err;

    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }
    err = parse_options(argc, argv, &options);
    if (err != 0) {
        usage(argv[0]);
        return 2;
    }
    if (options.notify_mode == HYDRA_NOTIFY_UMWAIT &&
        hydra_effective_notify_mode(options.notify_mode) !=
            HYDRA_NOTIFY_UMWAIT) {
        fprintf(stderr, "HydraRPC umwait requested but WAITPKG is unavailable\n");
        return 1;
    }
    if (strcmp(argv[1], "graph-check") == 0) {
        return run_graph_check(&options);
    }
    if (strcmp(argv[1], "template") == 0) {
        if (options.cxlfork.key == 0) {
            fprintf(stderr, "template requires --key=N\n");
            return 2;
        }
        return run_template(&options);
    }
    if (strcmp(argv[1], "inspect") == 0) {
        if (options.cxlfork.key == 0) {
            fprintf(stderr, "inspect requires --key=N\n");
            return 2;
        }
        err = pr_cxlfork_inspect(options.cxlfork.key,
                                 options.cxlfork.generation, true);
        if (err != 0) {
            print_error("inspect PageRank CXLfork template", err);
            return 1;
        }
        return 0;
    }
    if (strcmp(argv[1], "release") == 0) {
        if (options.cxlfork.key == 0 || options.cxlfork.generation == 0) {
            fprintf(stderr, "release requires --key=N --generation=N\n");
            return 2;
        }
        err = pr_cxlfork_release(options.cxlfork.key,
                                 options.cxlfork.generation);
        if (err != 0) {
            print_error("release PageRank CXLfork template", err);
            return 1;
        }
        return 0;
    }
    if (strcmp(argv[1], "coordinator") == 0) {
        if (options.tx_path == NULL || options.rx_path == NULL ||
            options.cpu_list == NULL) {
            usage(argv[0]);
            return 2;
        }
        return run_coordinator(&options);
    }
    if (strcmp(argv[1], "worker") == 0) {
        if (options.tx_path == NULL || options.rx_path == NULL ||
            options.cpu_list == NULL ||
            (options.state_mode == PR_STATE_CXLFORK &&
             (options.cxlfork.key == 0 ||
              options.cxlfork.generation == 0 ||
              options.cxlfork.page_checksum == 0))) {
            usage(argv[0]);
            return 2;
        }
        return run_worker(&options);
    }
    usage(argv[0]);
    return 2;
}
