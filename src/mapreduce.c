#define _GNU_SOURCE

#include "mapreduce.h"

#include "mr_hydra_transport.h"
#include "mr_kv.h"
#include "mr_util.h"
#include "mr_wire.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MR_DEFAULT_PAYLOAD_MAX (1024U * 1024U)
#define MR_DEFAULT_QUEUE_CAPACITY 4096U
#define MR_DEFAULT_MAP_SIZE (64ULL * 1024ULL * 1024ULL)

typedef enum emit_mode {
    EMIT_NONE,
    EMIT_MAP,
    EMIT_REDUCE,
} emit_mode;

typedef struct emit_context {
    emit_mode mode;
    mr_partitions *partitions;
    Partitioner partitioner;
} emit_context;

typedef struct str_list {
    char **items;
    size_t len;
    size_t cap;
} str_list;

typedef struct task_slice {
    const char *data;
    size_t len;
} task_slice;

typedef struct master_args {
    mr_backend backend;
    str_list workers;
    const char *input;
    const char *output;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t map_chunk;
    uint64_t reducers;
    bool quiet;
} master_args;

typedef struct worker_args {
    mr_backend backend;
    const char *path;
    uint64_t worker_id;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    bool create;
    bool quiet;
} worker_args;

typedef struct shuffle_accumulator {
    bool active;
    uint64_t job_id;
    uint32_t reducer_id;
    uint64_t partition_seq;
    uint32_t next_chunk_seq;
    byte_buf payload;
} shuffle_accumulator;

static emit_context current_emit;
static mr_partitions *current_reduce_input;
static int current_reduce_partition_number;

static const char *arg_value(const char *arg, const char *prefix)
{
    size_t len = strlen(prefix);
    return strncmp(arg, prefix, len) == 0 ? arg + len : NULL;
}

static void str_list_push(str_list *list, const char *value)
{
    if (list->len == list->cap) {
        list->cap = list->cap ? list->cap * 2 : 4;
        list->items = mr_xrealloc(list->items, list->cap * sizeof(list->items[0]));
    }
    list->items[list->len++] = mr_xstrdup(value);
}

static void str_list_add_csv(str_list *list, const char *csv)
{
    const char *start = csv;
    for (const char *p = csv;; p++) {
        if (*p == ',' || *p == '\0') {
            if (p > start) {
                char *item = mr_xstrndup(start, (size_t)(p - start));
                str_list_push(list, item);
                free(item);
            }
            if (*p == '\0') {
                break;
            }
            start = p + 1;
        }
    }
}

static void str_list_destroy(str_list *list)
{
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

void MR_Emit(char *key, char *value)
{
    if (current_emit.mode == EMIT_NONE || current_emit.partitions == NULL) {
        fprintf(stderr, "MR_Emit called outside map/reduce task\n");
        exit(1);
    }

    int partition_number = 0;
    if (current_emit.mode == EMIT_MAP) {
        int count = current_emit.partitions->count;
        unsigned long hashed = current_emit.partitioner(key, count);
        partition_number = (int)(hashed % (unsigned long)count);
    }

    mr_partitions_emit(current_emit.partitions, partition_number, key, value);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;

    while ((c = *key++) != '\0') {
        hash = hash * 33 + (unsigned char)c;
    }

    return hash % (unsigned long)num_partitions;
}

static char *reduce_get_next(char *key, int partition_number)
{
    if (partition_number != current_reduce_partition_number) {
        return NULL;
    }

    return mr_partitions_get_next(current_reduce_input, key, 0);
}

static void set_emit_context(emit_mode mode, mr_partitions *partitions,
                             Partitioner partitioner)
{
    current_emit.mode = mode;
    current_emit.partitions = partitions;
    current_emit.partitioner = partitioner ? partitioner : MR_DefaultHashPartition;
}

static void clear_emit_context(void)
{
    memset(&current_emit, 0, sizeof(current_emit));
}

static char *read_file(const char *path, size_t *out_len)
{
    FILE *fp = fopen(path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "open input %s failed: %s\n", path, strerror(errno));
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fprintf(stderr, "seek input failed: %s\n", strerror(errno));
        fclose(fp);
        return NULL;
    }
    long size = ftell(fp);
    if (size < 0) {
        fprintf(stderr, "tell input failed: %s\n", strerror(errno));
        fclose(fp);
        return NULL;
    }
    rewind(fp);

    char *data = mr_xcalloc((size_t)size + 1, 1);
    size_t got = fread(data, 1, (size_t)size, fp);
    if (got != (size_t)size) {
        fprintf(stderr, "read input failed\n");
        free(data);
        fclose(fp);
        return NULL;
    }

    fclose(fp);
    *out_len = (size_t)size;
    return data;
}

static task_slice *make_slices(const char *data, size_t len, size_t chunk,
                               size_t *out_count)
{
    task_slice *slices = NULL;
    size_t count = 0;
    size_t cap = 0;
    size_t pos = 0;

    while (pos < len) {
        size_t end = pos + chunk;
        if (end > len) {
            end = len;
        } else {
            size_t split = end;
            while (split > pos && !isspace((unsigned char)data[split - 1])) {
                split--;
            }
            if (split > pos) {
                end = split;
            }
        }
        if (end == pos) {
            end = pos + chunk;
            if (end > len) {
                end = len;
            }
        }

        if (count == cap) {
            cap = cap ? cap * 2 : 16;
            slices = mr_xrealloc(slices, cap * sizeof(slices[0]));
        }
        slices[count++] = (task_slice){.data = data + pos, .len = end - pos};
        pos = end;
    }

    *out_count = count;
    return slices;
}

static int send_task(mr_endpoint *ep, mr_task_kind kind, uint64_t task_id,
                     uint64_t partition_id, uint64_t partition_count,
                     const void *data, size_t len)
{
    byte_buf wire = mr_wire_make(kind, task_id, partition_id, partition_count,
                                 data, len);
    int err = mr_hydra_send_request_poll(ep->ctx, task_id, &wire);
    byte_buf_clear(&wire);
    if (err == 0) {
        ep->busy = true;
        ep->task_id = task_id;
    }
    return err;
}

static int collect_one_response(mr_endpoint *eps, size_t worker_count,
                                size_t *worker_index, mr_wire_header *hdr,
                                byte_buf *body)
{
    for (;;) {
        for (size_t i = 0; i < worker_count; i++) {
            if (!eps[i].busy) {
                continue;
            }

            uint64_t offset = 0;
            const hydra_msg *msg = NULL;
            const uint8_t *payload = NULL;
            int err = mr_hydra_recv_response(eps[i].ctx, &offset, &msg, &payload);
            if (err == EAGAIN) {
                continue;
            }
            if (err != 0) {
                fprintf(stderr, "master: recv response from worker %zu failed: %s\n",
                        i, hydra_strerror(err));
                return err;
            }

            const char *wire_body = NULL;
            size_t wire_body_len = 0;
            err = mr_wire_parse(payload, msg->len, hdr, &wire_body, &wire_body_len);
            if (err == 0) {
                byte_buf_append(body, wire_body, wire_body_len);
            }
            uint32_t status = msg->status;
            int release_err = hydra_release_message(eps[i].ctx, offset);
            eps[i].busy = false;
            *worker_index = i;
            if (release_err != 0) {
                return release_err;
            }
            if (err != 0 || status != 0 || hdr->kind == MR_TASK_ERROR) {
                if (body->len > 0) {
                    fprintf(stderr, "master: worker %zu error: %.*s\n",
                            i, (int)body->len, body->data);
                }
                return err != 0 ? err : EPROTO;
            }
            return 0;
        }
        hydra_cpu_relax();
    }
}

static int shutdown_workers(mr_endpoint *eps, size_t worker_count)
{
    for (;;) {
        bool has_busy_worker = false;
        for (size_t i = 0; i < worker_count; i++) {
            if (eps[i].ctx != NULL && eps[i].busy) {
                has_busy_worker = true;
                break;
            }
        }
        if (!has_busy_worker) {
            break;
        }

        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        (void)collect_one_response(eps, worker_count, &worker_index, &hdr, &body);
        byte_buf_clear(&body);
    }

    for (size_t i = 0; i < worker_count; i++) {
        if (eps[i].ctx != NULL && !eps[i].busy) {
            int err = send_task(&eps[i], MR_TASK_SHUTDOWN, UINT64_MAX - i, 0, 0,
                                "", 0);
            if (err != 0) {
                fprintf(stderr, "master: shutdown send to worker %zu failed: %s\n",
                        i, hydra_strerror(err));
            }
        }
    }

    for (size_t i = 0; i < worker_count; i++) {
        if (eps[i].ctx == NULL || !eps[i].busy) {
            continue;
        }
        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        (void)collect_one_response(eps, worker_count, &worker_index, &hdr, &body);
        byte_buf_clear(&body);
    }
    return 0;
}

static int worker_map(Mapper map, Partitioner partitioner, const char *data,
                      size_t len, int partition_count, size_t payload_limit,
                      byte_buf *out)
{
    mr_partitions partitions = {0};
    mr_partitions_init(&partitions, partition_count);
    set_emit_context(EMIT_MAP, &partitions, partitioner);
    map(data, len);
    clear_emit_context();

    mr_partitions_combine_counts(&partitions);
    int err = mr_partitions_serialize(&partitions, out, payload_limit);
    mr_partitions_destroy(&partitions);
    return err;
}

static int worker_reduce(Reducer reduce, const char *data, size_t len,
                         uint64_t partition_id, size_t payload_limit,
                         byte_buf *out)
{
    mr_partitions input = {0};
    mr_partitions output = {0};
    mr_partitions_init(&input, 1);
    mr_partitions_init(&output, 1);

    int err = mr_partitions_parse(&input, data, len, false, 0);
    if (err == 0) {
        mr_partitions_sort(&input);
        current_reduce_input = &input;
        current_reduce_partition_number = (int)partition_id;
        set_emit_context(EMIT_REDUCE, &output, MR_DefaultHashPartition);

        mr_partition *partition = &input.items[0];
        while (partition->cursor < partition->len) {
            char *key = partition->pairs[partition->cursor].key;
            reduce(key, reduce_get_next, (int)partition_id);
            while (partition->cursor < partition->len &&
                   strcmp(partition->pairs[partition->cursor].key, key) == 0) {
                partition->cursor++;
            }
        }

        clear_emit_context();
        current_reduce_input = NULL;
        current_reduce_partition_number = 0;
        err = mr_partitions_serialize(&output, out, payload_limit);
    }

    mr_partitions_destroy(&output);
    mr_partitions_destroy(&input);
    return err;
}

static void shuffle_accumulator_reset(shuffle_accumulator *acc)
{
    byte_buf_clear(&acc->payload);
    memset(acc, 0, sizeof(*acc));
}

static int parse_shuffle_chunk(const char *data, size_t len,
                               ShuffleChunkHeader *chunk,
                               const char **payload, size_t *payload_len)
{
    if (len < sizeof(*chunk)) {
        return EINVAL;
    }

    memcpy(chunk, data, sizeof(*chunk));
    if (chunk->payload_bytes > len - sizeof(*chunk)) {
        return EINVAL;
    }
    if (chunk->flags & ~(SHUFFLE_CHUNK_FIRST | SHUFFLE_CHUNK_LAST)) {
        return EINVAL;
    }

    *payload = data + sizeof(*chunk);
    *payload_len = chunk->payload_bytes;
    return 0;
}

static int worker_reduce_chunk(Reducer reduce, shuffle_accumulator *acc,
                               const char *data, size_t len,
                               size_t payload_limit, byte_buf *out)
{
    ShuffleChunkHeader chunk = {0};
    const char *payload = NULL;
    size_t payload_len = 0;
    int err = parse_shuffle_chunk(data, len, &chunk, &payload, &payload_len);
    if (err != 0) {
        return err;
    }

    bool first = (chunk.flags & SHUFFLE_CHUNK_FIRST) != 0;
    bool last = (chunk.flags & SHUFFLE_CHUNK_LAST) != 0;

    if (first) {
        shuffle_accumulator_reset(acc);
        acc->active = true;
        acc->job_id = chunk.job_id;
        acc->reducer_id = chunk.reducer_id;
        acc->partition_seq = chunk.partition_seq;
        acc->next_chunk_seq = 0;
    }

    if (!acc->active || chunk.job_id != acc->job_id ||
        chunk.reducer_id != acc->reducer_id ||
        chunk.partition_seq != acc->partition_seq ||
        chunk.chunk_seq != acc->next_chunk_seq) {
        shuffle_accumulator_reset(acc);
        return EPROTO;
    }

    byte_buf_append(&acc->payload, payload, payload_len);
    acc->next_chunk_seq++;

    if (!last) {
        return 0;
    }

    err = worker_reduce(reduce, acc->payload.data ? acc->payload.data : "",
                        acc->payload.len, chunk.reducer_id, payload_limit, out);
    shuffle_accumulator_reset(acc);
    return err;
}

static int send_reduce_chunk(mr_endpoint *eps, size_t worker_count,
                             size_t endpoint_index, uint64_t task_id,
                             uint64_t job_id, int reducer_id,
                             int reducer_count, uint64_t partition_seq,
                             uint32_t chunk_seq, uint32_t flags,
                             const byte_buf *payload, uint32_t record_count,
                             mr_partitions *final_output)
{
    if (payload->len > UINT32_MAX) {
        return EMSGSIZE;
    }

    ShuffleChunkHeader chunk = {
        .job_id = job_id,
        .mapper_id = 0,
        .reducer_id = (uint32_t)reducer_id,
        .partition_seq = partition_seq,
        .chunk_seq = chunk_seq,
        .flags = flags,
        .record_count = record_count,
        .payload_bytes = (uint32_t)payload->len,
    };

    byte_buf body = {0};
    byte_buf_append(&body, &chunk, sizeof(chunk));
    if (payload->len > 0) {
        byte_buf_append(&body, payload->data, payload->len);
    }

    int err = send_task(&eps[endpoint_index], MR_TASK_REDUCE, task_id,
                        (uint64_t)reducer_id, (uint64_t)reducer_count,
                        body.data, body.len);
    byte_buf_clear(&body);
    if (err != 0) {
        return err;
    }

    size_t worker_index = 0;
    mr_wire_header hdr = {0};
    byte_buf response = {0};
    err = collect_one_response(eps, worker_count, &worker_index, &hdr, &response);
    if (err != 0) {
        byte_buf_clear(&response);
        return err;
    }
    (void)worker_index;
    if (hdr.kind != MR_TASK_REDUCE ||
        hdr.task_id != task_id || hdr.partition_id != (uint64_t)reducer_id) {
        byte_buf_clear(&response);
        return EPROTO;
    }

    if (flags & SHUFFLE_CHUNK_LAST) {
        err = mr_partitions_parse(final_output,
                                  response.data ? response.data : "",
                                  response.len, false, reducer_id);
    }
    byte_buf_clear(&response);
    return err;
}

static int send_reduce_partition_chunks(mr_endpoint *eps, size_t worker_count,
                                        size_t endpoint_index,
                                        const mr_partitions *intermediate,
                                        int reducer_id, int reducer_count,
                                        size_t chunk_payload_limit,
                                        uint64_t *next_task_id,
                                        mr_partitions *final_output)
{
    const mr_partition *partition = &intermediate->items[reducer_id];
    uint64_t job_id = 1;
    uint64_t partition_seq = (uint64_t)reducer_id;
    uint32_t chunk_seq = 0;
    size_t pair_index = 0;

    if (partition->len == 0) {
        byte_buf empty = {0};
        return send_reduce_chunk(eps, worker_count, endpoint_index,
                                 (*next_task_id)++, job_id, reducer_id,
                                 reducer_count, partition_seq, chunk_seq,
                                 SHUFFLE_CHUNK_FIRST | SHUFFLE_CHUNK_LAST,
                                 &empty, 0, final_output);
    }

    while (pair_index < partition->len) {
        byte_buf payload = {0};
        uint32_t record_count = 0;
        bool first = chunk_seq == 0;

        while (pair_index < partition->len) {
            const mr_kv_pair *pair = &partition->pairs[pair_index];
            size_t record_size = mr_kv_pair_wire_size(pair);
            if (record_size > chunk_payload_limit) {
                byte_buf_clear(&payload);
                return EMSGSIZE;
            }
            if (payload.len > 0 &&
                payload.len + record_size > chunk_payload_limit) {
                break;
            }

            int err = mr_kv_pair_serialize(&payload, (uint64_t)reducer_id,
                                           pair, 0);
            if (err != 0) {
                byte_buf_clear(&payload);
                return err;
            }
            record_count++;
            pair_index++;

            if (payload.len >= chunk_payload_limit) {
                break;
            }
        }

        uint32_t flags = 0;
        if (first) {
            flags |= SHUFFLE_CHUNK_FIRST;
        }
        if (pair_index == partition->len) {
            flags |= SHUFFLE_CHUNK_LAST;
        }

        int err = send_reduce_chunk(eps, worker_count, endpoint_index,
                                    (*next_task_id)++, job_id, reducer_id,
                                    reducer_count, partition_seq, chunk_seq,
                                    flags, &payload, record_count,
                                    final_output);
        byte_buf_clear(&payload);
        if (err != 0) {
            return err;
        }
        chunk_seq++;
    }

    return 0;
}

static int run_worker(const worker_args *args, Mapper map, Reducer reduce,
                      Partitioner partitioner)
{
    hydra_ctx *ctx = NULL;
    int err = mr_hydra_open(&ctx, args->backend, args->path, args->map_size,
                            args->queue_capacity, args->payload_max, args->create);
    if (err != 0) {
        fprintf(stderr, "worker: open %s backend %s failed: %s\n",
                mr_backend_name(args->backend), args->path, hydra_strerror(err));
        return 1;
    }

    if (!args->quiet) {
        printf("worker[%" PRIu64 "]: backend=%s path=%s payload_max=%" PRIu64
               " queue=%" PRIu64 " notify=hydrarpc-poll\n",
               args->worker_id, mr_backend_name(args->backend), args->path,
               hydra_payload_max(ctx), hydra_queue_capacity(ctx));
        fflush(stdout);
    }

    shuffle_accumulator shuffle = {0};

    for (;;) {
        uint64_t offset = 0;
        const hydra_msg *msg = NULL;
        const uint8_t *payload = NULL;
        err = mr_hydra_recv_request_poll(ctx, &offset, &msg, &payload);
        if (err != 0) {
            fprintf(stderr, "worker: recv request failed: %s\n", hydra_strerror(err));
            hydra_close(ctx);
            return 1;
        }

        mr_wire_header hdr = {0};
        const char *body = NULL;
        size_t body_len = 0;
        err = mr_wire_parse(payload, msg->len, &hdr, &body, &body_len);
        byte_buf response_body = {0};
        mr_task_kind response_kind = (mr_task_kind)hdr.kind;
        uint32_t status = 0;

        if (err != 0) {
            const char *text = "invalid wire payload\n";
            byte_buf_append(&response_body, text, strlen(text));
            response_kind = MR_TASK_ERROR;
            status = 1;
        } else if (hdr.kind == MR_TASK_SHUTDOWN) {
            byte_buf wire = mr_wire_make(MR_TASK_SHUTDOWN, hdr.task_id, 0, 0, "", 0);
            err = mr_hydra_send_response_poll(ctx, msg, 0, &wire);
            byte_buf_clear(&wire);
            hydra_release_message(ctx, offset);
            shuffle_accumulator_reset(&shuffle);
            hydra_close(ctx);
            return err == 0 ? 0 : 1;
        } else if (hdr.kind == MR_TASK_MAP) {
            err = worker_map(map, partitioner, body, body_len,
                             (int)hdr.partition_count,
                             (size_t)(args->payload_max - sizeof(mr_wire_header)),
                             &response_body);
        } else if (hdr.kind == MR_TASK_REDUCE) {
            err = worker_reduce_chunk(reduce, &shuffle, body, body_len,
                                      (size_t)(args->payload_max -
                                               sizeof(mr_wire_header)),
                                      &response_body);
        } else {
            err = EINVAL;
        }

        if (err != 0) {
            byte_buf_clear(&response_body);
            const char *text = err == EMSGSIZE
                                   ? "task response exceeds payload-max\n"
                                   : "task failed\n";
            byte_buf_append(&response_body, text, strlen(text));
            response_kind = MR_TASK_ERROR;
            status = 1;
            shuffle_accumulator_reset(&shuffle);
        }

        byte_buf wire = mr_wire_make(response_kind, hdr.task_id, hdr.partition_id,
                                     hdr.partition_count, response_body.data,
                                     response_body.len);
        err = mr_hydra_send_response_poll(ctx, msg, status, &wire);
        byte_buf_clear(&response_body);
        byte_buf_clear(&wire);
        int release_err = hydra_release_message(ctx, offset);
        if (err != 0 || release_err != 0) {
            fprintf(stderr, "worker: send/release failed: %s\n",
                    err ? hydra_strerror(err) : hydra_strerror(release_err));
            hydra_close(ctx);
            return 1;
        }
    }
}

static int run_master(const master_args *args)
{
    if (args->workers.len == 0 || args->input == NULL || args->output == NULL) {
        fprintf(stderr, "master requires --input, --output and at least one --worker/--workers\n");
        return 2;
    }
    if (args->payload_max <= sizeof(mr_wire_header) + 64) {
        fprintf(stderr, "payload-max is too small\n");
        return 2;
    }

    size_t max_body = (size_t)(args->payload_max - sizeof(mr_wire_header));
    if (args->reducers > INT32_MAX || args->workers.len > INT32_MAX) {
        fprintf(stderr, "too many reducers/workers\n");
        return 2;
    }

    int reducer_count = args->reducers ? (int)args->reducers : (int)args->workers.len;
    if (reducer_count <= 0) {
        fprintf(stderr, "reducers must be positive\n");
        return 2;
    }

    int rc = 1;
    mr_endpoint *eps = NULL;
    char *input = NULL;
    task_slice *slices = NULL;
    mr_partitions intermediate = {0};
    mr_partitions final_output = {0};
    bool intermediate_init = false;
    bool final_init = false;
    bool should_shutdown = false;

    eps = mr_xcalloc(args->workers.len, sizeof(eps[0]));
    for (size_t i = 0; i < args->workers.len; i++) {
        eps[i].path = args->workers.items[i];
        int err = mr_hydra_open(&eps[i].ctx, args->backend, eps[i].path,
                                args->map_size, args->queue_capacity,
                                args->payload_max, true);
        if (err != 0) {
            fprintf(stderr, "master: open worker %zu path %s failed: %s\n",
                    i, eps[i].path, hydra_strerror(err));
            goto cleanup;
        }
        should_shutdown = true;
    }

    size_t input_len = 0;
    input = read_file(args->input, &input_len);
    if (input == NULL) {
        goto cleanup;
    }

    size_t chunk_size = args->map_chunk ? (size_t)args->map_chunk : max_body / 4;
    if (chunk_size == 0 || chunk_size > max_body) {
        chunk_size = max_body;
    }

    size_t slice_count = 0;
    slices = make_slices(input, input_len, chunk_size, &slice_count);
    mr_partitions_init(&intermediate, reducer_count);
    intermediate_init = true;

    if (!args->quiet) {
        printf("master: workers=%zu reducers=%d backend=%s payload_max=%" PRIu64
               " map_chunks=%zu notify=hydrarpc-poll\n",
               args->workers.len, reducer_count, mr_backend_name(args->backend),
               args->payload_max, slice_count);
        fflush(stdout);
    }

    size_t next_slice = 0;
    size_t completed = 0;
    while (completed < slice_count) {
        for (size_t i = 0; i < args->workers.len && next_slice < slice_count; i++) {
            if (!eps[i].busy) {
                int err = send_task(&eps[i], MR_TASK_MAP, next_slice, 0,
                                    (uint64_t)reducer_count,
                                    slices[next_slice].data, slices[next_slice].len);
                if (err != 0) {
                    fprintf(stderr, "master: send map task failed: %s\n",
                            hydra_strerror(err));
                    goto cleanup;
                }
                next_slice++;
            }
        }

        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        int err = collect_one_response(eps, args->workers.len, &worker_index,
                                       &hdr, &body);
        if (err != 0) {
            byte_buf_clear(&body);
            goto cleanup;
        }
        if (hdr.kind != MR_TASK_MAP) {
            fprintf(stderr, "master: unexpected map response kind %u\n", hdr.kind);
            byte_buf_clear(&body);
            goto cleanup;
        }
        err = mr_partitions_parse(&intermediate, body.data ? body.data : "",
                                  body.len, true, 0);
        byte_buf_clear(&body);
        if (err != 0) {
            fprintf(stderr, "master: parse map response failed\n");
            goto cleanup;
        }
        completed++;
        (void)worker_index;
    }

    mr_partitions_sort(&intermediate);
    if (max_body <= sizeof(ShuffleChunkHeader)) {
        fprintf(stderr, "payload-max is too small for shuffle chunk header\n");
        goto cleanup;
    }

    mr_partitions_init(&final_output, reducer_count);
    final_init = true;
    size_t chunk_payload_limit = max_body - sizeof(ShuffleChunkHeader);
    uint64_t next_reduce_task_id = slice_count;
    for (int i = 0; i < reducer_count; i++) {
        size_t endpoint_index = (size_t)i % args->workers.len;
        int err = send_reduce_partition_chunks(eps, args->workers.len,
                                               endpoint_index, &intermediate,
                                               i, reducer_count,
                                               chunk_payload_limit,
                                               &next_reduce_task_id,
                                               &final_output);
        if (err != 0) {
            fprintf(stderr, "master: reduce partition %d shuffle failed: %s\n",
                    i, strerror(err));
            goto cleanup;
        }
    }

    rc = mr_partitions_write_text(&final_output, args->output);
    if (!args->quiet && rc == 0) {
        printf("master: wrote %s with %d reducer partition(s)\n",
               args->output, reducer_count);
    }

cleanup:
    if (should_shutdown) {
        shutdown_workers(eps, args->workers.len);
    }
    if (eps != NULL) {
        for (size_t i = 0; i < args->workers.len; i++) {
            if (eps[i].ctx != NULL) {
                hydra_close(eps[i].ctx);
            }
        }
    }
    if (final_init) {
        mr_partitions_destroy(&final_output);
    }
    if (intermediate_init) {
        mr_partitions_destroy(&intermediate);
    }
    free(slices);
    free(input);
    free(eps);
    return rc;
}

static void usage(const char *argv0)
{
    fprintf(stderr,
            "usage:\n"
            "  %s master --input=FILE --output=FILE --worker=PATH [--worker=PATH...]\n"
            "  %s master --input=FILE --output=FILE --workers=PATH,PATH\n"
            "  %s worker --path=PATH [--worker-id=N]\n"
            "\n"
            "common: [--backend=file|devdax|ivshmem] [--payload-max=N]\n"
            "        [--queue-capacity=N] [--size=N] [--quiet]\n"
            "master: [--map-chunk=N] [--reducers=N]\n"
            "worker: [--create]\n",
            argv0, argv0, argv0);
}

static int parse_master_args(master_args *args, int argc, char **argv)
{
    args->backend = MR_BACKEND_FILE;
    args->map_size = MR_DEFAULT_MAP_SIZE;
    args->queue_capacity = MR_DEFAULT_QUEUE_CAPACITY;
    args->payload_max = MR_DEFAULT_PAYLOAD_MAX;

    for (int i = 2; i < argc; i++) {
        const char *value = NULL;
        if ((value = arg_value(argv[i], "--backend=")) != NULL) {
            args->backend = mr_backend_parse(value);
        } else if ((value = arg_value(argv[i], "--input=")) != NULL) {
            args->input = value;
        } else if ((value = arg_value(argv[i], "--output=")) != NULL) {
            args->output = value;
        } else if ((value = arg_value(argv[i], "--worker=")) != NULL) {
            str_list_push(&args->workers, value);
        } else if ((value = arg_value(argv[i], "--workers=")) != NULL) {
            str_list_add_csv(&args->workers, value);
        } else if ((value = arg_value(argv[i], "--payload-max=")) != NULL) {
            args->payload_max = mr_parse_u64(value, "payload-max");
        } else if ((value = arg_value(argv[i], "--queue-capacity=")) != NULL) {
            args->queue_capacity = mr_parse_u64(value, "queue-capacity");
        } else if ((value = arg_value(argv[i], "--size=")) != NULL) {
            args->map_size = mr_parse_u64(value, "size");
        } else if ((value = arg_value(argv[i], "--map-chunk=")) != NULL) {
            args->map_chunk = mr_parse_u64(value, "map-chunk");
        } else if ((value = arg_value(argv[i], "--reducers=")) != NULL) {
            args->reducers = mr_parse_u64(value, "reducers");
        } else if (strcmp(argv[i], "--quiet") == 0) {
            args->quiet = true;
        } else {
            fprintf(stderr, "unknown master arg: %s\n", argv[i]);
            return 2;
        }
    }
    return 0;
}

static int parse_worker_args(worker_args *args, int argc, char **argv)
{
    args->backend = MR_BACKEND_FILE;
    args->map_size = MR_DEFAULT_MAP_SIZE;
    args->queue_capacity = MR_DEFAULT_QUEUE_CAPACITY;
    args->payload_max = MR_DEFAULT_PAYLOAD_MAX;

    for (int i = 2; i < argc; i++) {
        const char *value = NULL;
        if ((value = arg_value(argv[i], "--backend=")) != NULL) {
            args->backend = mr_backend_parse(value);
        } else if ((value = arg_value(argv[i], "--path=")) != NULL) {
            args->path = value;
        } else if ((value = arg_value(argv[i], "--worker-id=")) != NULL) {
            args->worker_id = mr_parse_u64(value, "worker-id");
        } else if ((value = arg_value(argv[i], "--payload-max=")) != NULL) {
            args->payload_max = mr_parse_u64(value, "payload-max");
        } else if ((value = arg_value(argv[i], "--queue-capacity=")) != NULL) {
            args->queue_capacity = mr_parse_u64(value, "queue-capacity");
        } else if ((value = arg_value(argv[i], "--size=")) != NULL) {
            args->map_size = mr_parse_u64(value, "size");
        } else if (strcmp(argv[i], "--create") == 0) {
            args->create = true;
        } else if (strcmp(argv[i], "--quiet") == 0) {
            args->quiet = true;
        } else {
            fprintf(stderr, "unknown worker arg: %s\n", argv[i]);
            return 2;
        }
    }
    if (args->path == NULL) {
        fprintf(stderr, "worker requires --path\n");
        return 2;
    }
    return 0;
}

int MR_Run(int argc, char *argv[], Mapper map, Reducer reduce,
           Partitioner partition)
{
    if (map == NULL || reduce == NULL || argc < 2) {
        usage(argv[0]);
        return 2;
    }

    if (strcmp(argv[1], "master") == 0) {
        master_args args = {0};
        int err = parse_master_args(&args, argc, argv);
        if (err != 0) {
            usage(argv[0]);
            str_list_destroy(&args.workers);
            return err;
        }
        int rc = run_master(&args);
        str_list_destroy(&args.workers);
        return rc;
    }

    if (strcmp(argv[1], "worker") == 0) {
        worker_args args = {0};
        int err = parse_worker_args(&args, argc, argv);
        if (err != 0) {
            usage(argv[0]);
            return err;
        }
        return run_worker(&args, map, reduce,
                          partition ? partition : MR_DefaultHashPartition);
    }

    usage(argv[0]);
    return 2;
}
