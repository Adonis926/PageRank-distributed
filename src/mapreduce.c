#define _GNU_SOURCE

#include "mapreduce.h"

#include "mr_cxlfork.h"
#include "mr_hydra_transport.h"
#include "mr_kv.h"
#include "mr_porter.h"
#include "mr_util.h"
#include "mr_wire.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MR_DEFAULT_PAYLOAD_MAX (1024U * 1024U)
#define MR_DEFAULT_QUEUE_CAPACITY 4096U
#define MR_DEFAULT_MAP_SIZE (64ULL * 1024ULL * 1024ULL)
#define MR_DEFAULT_WORKERS_PER_AGENT 1U
#define MR_DEFAULT_SHUFFLE_CONNECTIONS 1U
#define MR_SERVICE_CONTROL 1U
#define MR_PATH_MAX 256U
#define MR_JOB_ID 1ULL
#define MR_AGENT_OPEN_RETRY_COUNT 10000U
#define MR_AGENT_OPEN_RETRY_US 1000U
#define MR_TARGET_WORKER_AUTO UINT32_MAX

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

typedef struct IntermediateLocation {
    uint64_t job_id;
    uint64_t map_task_id;
    uint32_t reducer_id;
    uint32_t node_id;
    uint32_t worker_id;
    uint32_t global_worker_id;
    uint64_t offset;
    uint64_t bytes;
    uint32_t record_count;
    uint32_t reserved;
    char path[MR_PATH_MAX];
    char shuffle_path[MR_PATH_MAX];
} IntermediateLocation;

typedef struct location_vec {
    IntermediateLocation *items;
    size_t len;
    size_t cap;
} location_vec;

typedef struct control_map_task {
    uint64_t job_id;
    uint64_t map_task_id;
    uint32_t reducer_count;
    uint32_t target_worker_id;
    uint64_t data_len;
} control_map_task;

typedef struct control_reduce_task {
    uint64_t job_id;
    uint32_t reducer_id;
    uint32_t reducer_count;
    uint32_t target_worker_id;
    uint32_t reserved;
    uint64_t location_count;
} control_reduce_task;

typedef struct control_locations_header {
    uint64_t count;
} control_locations_header;

typedef struct shuffle_fetch_request {
    uint64_t job_id;
    uint64_t map_task_id;
    uint32_t reducer_id;
    uint32_t reserved;
    uint64_t offset;
    uint64_t bytes;
    char path[MR_PATH_MAX];
} shuffle_fetch_request;

typedef struct master_args {
    mr_backend backend;
    str_list agents;
    const char *input;
    const char *output;
    const char *schedule;
    const char *ready_file;
    const char *start_file;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t map_chunk;
    uint64_t reducers;
    uint64_t connections;
    uint64_t workers_per_agent;
    uint64_t activation_probes;
    uint64_t activation_warmups;
    uint64_t probe_agent;
    uint64_t probe_state_checksum;
    mr_worker_role probe_role;
    mr_queue_policy queue_policy;
    bool quiet;
} master_args;

typedef struct schedule_entry {
    mr_task_kind kind;
    uint64_t task_id;
    uint32_t node_id;
    uint32_t worker_id;
} schedule_entry;

typedef struct schedule_table {
    schedule_entry *items;
    size_t len;
    size_t cap;
} schedule_table;

typedef struct agent_args {
    mr_backend backend;
    mr_backend shuffle_backend;
    const char *path;
    const char *store_dir;
    const char *ready_file;
    uint64_t node_id;
    uint64_t agent_count;
    uint64_t global_worker_base;
    uint64_t workers;
    uint64_t map_size;
    uint64_t shuffle_map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t shuffle_connections;
    mr_queue_policy shuffle_queue_policy;
    mr_worker_role role;
    mr_launch_mode launch_mode;
    uint64_t template_base;
    uint64_t template_bytes;
    uint64_t template_seed;
    uint32_t template_value_scale;
    uint64_t template_key;
    uint64_t template_generation;
    uint64_t template_page_checksum;
    mr_cxlfork_policy cxlfork_policy;
    uint64_t restore_hold_ms;
    bool validate_full;
    bool restore_preopen;
    bool lazy_porter;
    bool create;
    bool quiet;
} agent_args;

typedef struct worker_child_args {
    Mapper map;
    Reducer reduce;
    Partitioner partitioner;
    mr_backend shuffle_backend;
    char store_dir[MR_PATH_MAX];
    char shuffle_path[MR_PATH_MAX];
    uint32_t node_id;
    uint32_t worker_id;
    uint32_t global_worker_id;
    uint32_t global_worker_base;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t shuffle_connections;
    mr_queue_policy shuffle_queue_policy;
} worker_child_args;

static emit_context current_emit;
static mr_partitions *current_reduce_input;
static int current_reduce_partition_number;
static const MR_WorkerStateV1 *current_worker_state;
static uint64_t current_worker_state_access_checksum;
static uint64_t current_worker_state_access_count;
static int current_worker_state_error;
static mr_task_trace_v3 *current_task_trace;

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
    memset(list, 0, sizeof(*list));
}

static void location_vec_push(location_vec *vec, const IntermediateLocation *loc)
{
    if (vec->len == vec->cap) {
        vec->cap = vec->cap ? vec->cap * 2 : 32;
        vec->items = mr_xrealloc(vec->items, vec->cap * sizeof(vec->items[0]));
    }
    vec->items[vec->len++] = *loc;
}

static void location_vec_destroy(location_vec *vec)
{
    free(vec->items);
    memset(vec, 0, sizeof(*vec));
}

static void schedule_table_push(schedule_table *table, const schedule_entry *entry)
{
    if (table->len == table->cap) {
        table->cap = table->cap ? table->cap * 2 : 32;
        table->items = mr_xrealloc(table->items,
                                   table->cap * sizeof(table->items[0]));
    }
    table->items[table->len++] = *entry;
}

static void schedule_table_destroy(schedule_table *table)
{
    free(table->items);
    memset(table, 0, sizeof(*table));
}

static char *trim_ascii(char *s)
{
    while (isspace((unsigned char)*s)) {
        s++;
    }
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) {
        *--end = '\0';
    }
    return s;
}

static int parse_u64_field(const char *text, uint64_t *out)
{
    if (*text == '-') {
        return EINVAL;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *trim_ascii(end) != '\0') {
        return EINVAL;
    }
    *out = (uint64_t)value;
    return 0;
}

static int parse_schedule_kind(const char *text, mr_task_kind *out)
{
    if (strcmp(text, "map") == 0 || strcmp(text, "m") == 0) {
        *out = MR_TASK_MAP;
        return 0;
    }
    if (strcmp(text, "reduce") == 0 || strcmp(text, "r") == 0) {
        *out = MR_TASK_REDUCE;
        return 0;
    }
    return EINVAL;
}

static int parse_schedule_file(const char *path, schedule_table *table)
{
    FILE *fp = fopen(path, "r");
    if (fp == NULL) {
        return errno;
    }

    char line[512];
    size_t line_no = 0;
    while (fgets(line, sizeof(line), fp) != NULL) {
        line_no++;
        char *comment = strchr(line, '#');
        if (comment != NULL) {
            *comment = '\0';
        }
        char *fields[4] = {0};
        size_t field_count = 0;
        char *save = NULL;
        for (char *tok = strtok_r(line, ",", &save); tok != NULL;
             tok = strtok_r(NULL, ",", &save)) {
            if (field_count >= 4) {
                fclose(fp);
                fprintf(stderr, "schedule %s:%zu has too many fields\n",
                        path, line_no);
                return EINVAL;
            }
            fields[field_count++] = trim_ascii(tok);
        }
        if (field_count == 0) {
            continue;
        }
        if (field_count == 4 && strcmp(fields[0], "kind") == 0) {
            continue;
        }
        if (field_count != 4) {
            fclose(fp);
            fprintf(stderr,
                    "schedule %s:%zu expected kind,id,node_id,worker_id\n",
                    path, line_no);
            return EINVAL;
        }

        schedule_entry entry = {0};
        uint64_t node_id = 0;
        uint64_t worker_id = 0;
        int err = parse_schedule_kind(fields[0], &entry.kind);
        if (err == 0) {
            err = parse_u64_field(fields[1], &entry.task_id);
        }
        if (err == 0) {
            err = parse_u64_field(fields[2], &node_id);
        }
        if (err == 0) {
            err = parse_u64_field(fields[3], &worker_id);
        }
        if (err != 0 || node_id > UINT32_MAX || worker_id > UINT32_MAX) {
            fclose(fp);
            fprintf(stderr, "schedule %s:%zu has invalid field\n", path, line_no);
            return EINVAL;
        }
        entry.node_id = (uint32_t)node_id;
        entry.worker_id = (uint32_t)worker_id;
        schedule_table_push(table, &entry);
    }

    if (ferror(fp)) {
        int err = errno ? errno : EIO;
        fclose(fp);
        return err;
    }
    fclose(fp);
    return 0;
}

static bool schedule_lookup(const schedule_table *table, mr_task_kind kind,
                            uint64_t task_id, schedule_entry *out)
{
    for (size_t i = 0; i < table->len; i++) {
        const schedule_entry *entry = &table->items[i];
        if (entry->kind == kind && entry->task_id == task_id) {
            *out = *entry;
            return true;
        }
    }
    return false;
}

static int validate_schedule_table(const schedule_table *table, size_t agent_count,
                                   size_t map_count, size_t reduce_count)
{
    for (size_t i = 0; i < table->len; i++) {
        const schedule_entry *entry = &table->items[i];
        if (entry->node_id >= agent_count) {
            fprintf(stderr, "schedule entry node_id=%u out of range\n",
                    entry->node_id);
            return EINVAL;
        }
        if (entry->kind == MR_TASK_MAP && entry->task_id >= map_count) {
            fprintf(stderr, "schedule map id=%" PRIu64 " out of range\n",
                    entry->task_id);
            return EINVAL;
        }
        if (entry->kind == MR_TASK_REDUCE && entry->task_id >= reduce_count) {
            fprintf(stderr, "schedule reduce id=%" PRIu64 " out of range\n",
                    entry->task_id);
            return EINVAL;
        }
        for (size_t j = i + 1; j < table->len; j++) {
            const schedule_entry *other = &table->items[j];
            if (entry->kind == other->kind && entry->task_id == other->task_id) {
                fprintf(stderr, "schedule duplicate entry for %s %" PRIu64 "\n",
                        entry->kind == MR_TASK_MAP ? "map" : "reduce",
                        entry->task_id);
                return EINVAL;
            }
        }
    }

    for (size_t i = 0; i < map_count; i++) {
        schedule_entry ignored = {0};
        if (!schedule_lookup(table, MR_TASK_MAP, i, &ignored)) {
            fprintf(stderr, "schedule missing map task %zu\n", i);
            return EINVAL;
        }
    }
    for (size_t i = 0; i < reduce_count; i++) {
        schedule_entry ignored = {0};
        if (!schedule_lookup(table, MR_TASK_REDUCE, i, &ignored)) {
            fprintf(stderr, "schedule missing reduce task %zu\n", i);
            return EINVAL;
        }
    }
    return 0;
}

static int mkdir_p(const char *path)
{
    char tmp[MR_PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) {
        return ENAMETOOLONG;
    }
    memcpy(tmp, path, len + 1);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
                return errno;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
        return errno;
    }
    return 0;
}

static char *read_file(const char *path, size_t *out_len)
{
    FILE *fp = fopen(path, "rb");
    if (fp == NULL) {
        fprintf(stderr, "open input %s failed: %s\n", path, strerror(errno));
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }
    rewind(fp);
    char *data = mr_xcalloc((size_t)size + 1, 1);
    if (fread(data, 1, (size_t)size, fp) != (size_t)size) {
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

const MR_WorkerStateV1 *MR_GetWorkerState(void)
{
    if (current_worker_state == NULL) {
        MR_TaskFail(EPROTO);
        return NULL;
    }
    current_worker_state_access_count++;
    if (current_task_trace != NULL &&
        current_task_trace->first_state_access_ns == 0) {
        current_task_trace->first_state_access_ns = hydra_now_ns();
    }
    current_worker_state_access_checksum ^=
        current_worker_state->checksum + UINT64_C(0x9e3779b97f4a7c15) +
        (current_worker_state_access_count << 6) +
        (current_worker_state_access_count >> 2);
    return current_worker_state;
}

void MR_TaskFail(int error)
{
    if (current_worker_state_error == 0) {
        current_worker_state_error = error != 0 ? error : EPROTO;
    }
}

static void begin_worker_state(const MR_WorkerStateV1 *state,
                               mr_worker_role expected_role,
                               mr_task_trace_v3 *trace)
{
    current_worker_state = state;
    current_worker_state_access_checksum = 0;
    current_worker_state_access_count = 0;
    current_worker_state_error = 0;
    current_task_trace = trace;
    if (state == NULL ||
        (state->role != MR_WORKER_ROLE_BOTH &&
         state->role != expected_role)) {
        MR_TaskFail(EACCES);
    }
}

static int finish_worker_state(mr_template_binding_result *binding)
{
    int err = current_worker_state_error;
    if (err == 0 && current_worker_state_access_count == 0) {
        err = ENODATA;
    }
    if (binding != NULL) {
        binding->access_checksum = current_worker_state_access_checksum;
        binding->access_count = current_worker_state_access_count;
        if (err == 0) {
            binding->flags |= MR_TEMPLATE_BIND_APP_CONSUMED;
        }
    }
    current_worker_state = NULL;
    current_worker_state_access_checksum = 0;
    current_worker_state_access_count = 0;
    current_worker_state_error = 0;
    current_task_trace = NULL;
    return err;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    const MR_WorkerStateV1 *state = MR_GetWorkerState();
    if (state == NULL || num_partitions <= 0) {
        MR_TaskFail(EINVAL);
        return 0;
    }
    uint64_t hash = state->hash_initial;
    int c;
    while ((c = *key++) != '\0') {
        hash = hash * state->hash_multiplier + (unsigned char)c;
    }
    return (unsigned long)(hash % (uint64_t)num_partitions);
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

static byte_buf make_wire(mr_task_kind kind, uint64_t task_id,
                          uint64_t partition_id, uint64_t partition_count,
                          const void *data, size_t len)
{
    return mr_wire_make(kind, task_id, partition_id, partition_count, data, len);
}

static int send_hydra_request_observed(
    hydra_ctx *ctx, mr_task_kind kind, uint64_t task_id,
    uint64_t partition_id, uint64_t partition_count,
    const void *data, size_t len, mr_hydra_send_observation *observation);

static int send_hydra_request(hydra_ctx *ctx, mr_task_kind kind, uint64_t task_id,
                              uint64_t partition_id, uint64_t partition_count,
                              const void *data, size_t len)
{
    return send_hydra_request_observed(ctx, kind, task_id, partition_id,
                                       partition_count, data, len, NULL);
}

static int send_hydra_request_observed(
    hydra_ctx *ctx, mr_task_kind kind, uint64_t task_id,
    uint64_t partition_id, uint64_t partition_count,
    const void *data, size_t len, mr_hydra_send_observation *observation)
{
    byte_buf wire = make_wire(kind, task_id, partition_id, partition_count, data, len);
    int err = mr_hydra_send_request_poll_observed(
        ctx, task_id, &wire, observation);
    byte_buf_clear(&wire);
    return err;
}

static int recv_hydra_response_observed(hydra_ctx *ctx, mr_wire_header *hdr,
                                        byte_buf *body, hydra_msg *message_out,
                                        uint64_t *receive_ns,
                                        uint64_t *release_done_ns)
{
    for (;;) {
        uint64_t offset = 0;
        const hydra_msg *msg = NULL;
        const uint8_t *payload = NULL;
        int err = mr_hydra_recv_response(ctx, &offset, &msg, &payload);
        if (err == EAGAIN) {
            hydra_cpu_relax();
            continue;
        }
        if (err != 0) {
            return err;
        }
        if (receive_ns != NULL) {
            *receive_ns = hydra_now_ns();
        }
        if (message_out != NULL) {
            *message_out = *msg;
        }

        const char *wire_body = NULL;
        size_t wire_body_len = 0;
        err = mr_wire_parse(payload, msg->len, hdr, &wire_body, &wire_body_len);
        if (err == 0) {
            byte_buf_append(body, wire_body, wire_body_len);
        }
        uint32_t status = msg->status;
        int release_err = hydra_release_message(ctx, offset);
        if (release_done_ns != NULL) {
            *release_done_ns = hydra_now_ns();
        }
        if (release_err != 0) {
            return release_err;
        }
        if (err != 0) {
            return err;
        }
        if (status != 0 &&
            (hdr->kind == MR_TASK_MAP || hdr->kind == MR_TASK_REDUCE ||
             hdr->kind == MR_TASK_ACTIVATION_PROBE) &&
            body->len >= sizeof(mr_task_result_v4)) {
            /*
             * Porter failures still carry a versioned measurement envelope.
             * Let the caller unwrap and report it instead of printing binary
             * payload as a string or discarding failure-path evidence.
             */
            return 0;
        }
        if (status != 0 || hdr->kind == MR_TASK_ERROR) {
            if (body->len > 0) {
                fprintf(stderr, "remote error: %.*s\n", (int)body->len, body->data);
            }
            return EPROTO;
        }
        return 0;
    }
}

static int recv_hydra_response(hydra_ctx *ctx, mr_wire_header *hdr,
                               byte_buf *body)
{
    return recv_hydra_response_observed(ctx, hdr, body, NULL, NULL, NULL);
}

static int write_partition_file(const char *path, const byte_buf *data)
{
    FILE *fp = fopen(path, "wb");
    if (fp == NULL) {
        return errno;
    }
    if (data->len > 0 && fwrite(data->data, 1, data->len, fp) != data->len) {
        int err = errno ? errno : EIO;
        fclose(fp);
        return err;
    }
    if (fclose(fp) != 0) {
        return errno;
    }
    return 0;
}

static int append_file_range(FILE *out, const char *path, uint64_t offset,
                             uint64_t bytes)
{
    FILE *fp = fopen(path, "rb");
    if (fp == NULL) {
        return errno;
    }
    if (fseeko(fp, (off_t)offset, SEEK_SET) != 0) {
        int err = errno;
        fclose(fp);
        return err;
    }

    char buf[65536];
    uint64_t remaining = bytes;
    while (remaining > 0) {
        size_t want = remaining < sizeof(buf) ? (size_t)remaining : sizeof(buf);
        size_t got = fread(buf, 1, want, fp);
        if (got == 0 && ferror(fp)) {
            int err = errno ? errno : EIO;
            fclose(fp);
            return err;
        }
        if (got == 0) {
            fclose(fp);
            return EIO;
        }
        if (fwrite(buf, 1, got, out) != got) {
            int err = errno ? errno : EIO;
            fclose(fp);
            return err;
        }
        remaining -= got;
    }
    fclose(fp);
    return 0;
}

static int worker_do_map(const worker_child_args *args, Mapper map,
                         Partitioner partitioner, const char *data, size_t len,
                         uint64_t job_id, uint64_t map_task_id,
                         int reducer_count, byte_buf *out,
                         mr_task_trace_v3 *trace)
{
    mr_partitions partitions = {0};
    mr_partitions_init(&partitions, reducer_count);
    set_emit_context(EMIT_MAP, &partitions, partitioner);
    trace->first_callback_entry_ns = hydra_now_ns();
    map(data, len);
    trace->callback_region_end_ns = hydra_now_ns();
    clear_emit_context();
    trace->serialize_begin_ns = hydra_now_ns();
    mr_partitions_combine_counts(&partitions);

    control_locations_header header = {.count = (uint64_t)reducer_count};
    byte_buf_append(out, &header, sizeof(header));

    for (int r = 0; r < reducer_count; r++) {
        byte_buf payload = {0};
        int err = mr_partition_serialize(&partitions, r, &payload, 0);
        if (err != 0) {
            byte_buf_clear(&payload);
            mr_partitions_destroy(&partitions);
            return err;
        }

        char path[MR_PATH_MAX];
        int n = snprintf(path, sizeof(path),
                         "%s/job-%" PRIu64 "-worker-%u-map-%" PRIu64 "-part-%d.kv",
                         args->store_dir, job_id, args->global_worker_id,
                         map_task_id, r);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            byte_buf_clear(&payload);
            mr_partitions_destroy(&partitions);
            return ENAMETOOLONG;
        }
        err = write_partition_file(path, &payload);
        if (err != 0) {
            byte_buf_clear(&payload);
            mr_partitions_destroy(&partitions);
            return err;
        }

        IntermediateLocation loc = {
            .job_id = job_id,
            .map_task_id = map_task_id,
            .reducer_id = (uint32_t)r,
            .node_id = args->node_id,
            .worker_id = args->worker_id,
            .global_worker_id = args->global_worker_id,
            .offset = 0,
            .bytes = payload.len,
            .record_count = (uint32_t)partitions.items[r].len,
        };
        snprintf(loc.path, sizeof(loc.path), "%s", path);
        snprintf(loc.shuffle_path, sizeof(loc.shuffle_path), "%s", args->shuffle_path);
        byte_buf_append(out, &loc, sizeof(loc));
        byte_buf_clear(&payload);
    }

    mr_partitions_destroy(&partitions);
    trace->serialize_end_ns = hydra_now_ns();
    return 0;
}

static int shuffle_fetch_remote(const worker_child_args *args,
                                const IntermediateLocation *loc, FILE *spill)
{
    uint64_t remaining = loc->bytes;
    uint64_t offset = loc->offset;
    size_t max_body = (size_t)(args->payload_max - sizeof(mr_wire_header));
    size_t max_chunk = max_body > sizeof(ShuffleChunkHeader)
                           ? max_body - sizeof(ShuffleChunkHeader)
                           : 0;
    if (max_chunk == 0) {
        return EMSGSIZE;
    }

    hydra_ctx *ctx = NULL;
    mr_hydra_open_config config = {
        .backend = args->shuffle_backend,
        .path = loc->shuffle_path,
        .map_size = args->map_size,
        .queue_capacity = args->queue_capacity,
        .payload_max = args->payload_max,
        .connections = args->shuffle_connections,
        .connection_id = args->worker_id % args->shuffle_connections,
        .node_id = 0,
        .peer_node_id = loc->node_id + 1,
        .service_id = MR_SERVICE_CONTROL,
        .queue_policy = args->shuffle_queue_policy,
        .create = false,
    };
    int err = mr_hydra_open_ex(&ctx, &config);
    if (err != 0) {
        return err;
    }

    uint32_t chunk_seq = 0;
    while (remaining > 0 || (loc->bytes == 0 && chunk_seq == 0)) {
        uint64_t want64 = remaining < max_chunk ? remaining : (uint64_t)max_chunk;
        shuffle_fetch_request req = {
            .job_id = loc->job_id,
            .map_task_id = loc->map_task_id,
            .reducer_id = loc->reducer_id,
            .offset = offset,
            .bytes = want64,
        };
        snprintf(req.path, sizeof(req.path), "%s", loc->path);

        err = send_hydra_request(ctx, MR_TASK_SHUFFLE_FETCH, chunk_seq,
                                 loc->reducer_id, 0, &req, sizeof(req));
        if (err != 0) {
            hydra_close(ctx);
            return err;
        }
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        err = recv_hydra_response(ctx, &hdr, &body);
        if (err != 0) {
            byte_buf_clear(&body);
            hydra_close(ctx);
            return err;
        }
        if (hdr.kind != MR_TASK_SHUFFLE_FETCH ||
            body.len < sizeof(ShuffleChunkHeader)) {
            byte_buf_clear(&body);
            hydra_close(ctx);
            return EPROTO;
        }
        ShuffleChunkHeader chunk = {0};
        memcpy(&chunk, body.data, sizeof(chunk));
        if (chunk.payload_bytes > body.len - sizeof(chunk)) {
            byte_buf_clear(&body);
            hydra_close(ctx);
            return EPROTO;
        }
        if (chunk.payload_bytes > 0 &&
            fwrite(body.data + sizeof(chunk), 1, chunk.payload_bytes, spill) !=
                chunk.payload_bytes) {
            byte_buf_clear(&body);
            hydra_close(ctx);
            return errno ? errno : EIO;
        }
        byte_buf_clear(&body);
        if (remaining == 0) {
            break;
        }
        remaining -= want64;
        offset += want64;
        chunk_seq++;
    }

    hydra_close(ctx);
    return 0;
}

static int worker_do_reduce(const worker_child_args *args, Reducer reduce,
                            uint32_t reducer_id, uint32_t reducer_count,
                            const IntermediateLocation *locations,
                            size_t location_count, byte_buf *out,
                            mr_task_trace_v3 *trace)
{
    char spill_path[MR_PATH_MAX];
    int n = snprintf(spill_path, sizeof(spill_path),
                     "%s/reduce-worker-%u-part-%u.spill",
                     args->store_dir, args->global_worker_id, reducer_id);
    if (n < 0 || (size_t)n >= sizeof(spill_path)) {
        return ENAMETOOLONG;
    }
    trace->shuffle_begin_ns = hydra_now_ns();
    FILE *spill = fopen(spill_path, "wb");
    if (spill == NULL) {
        return errno;
    }

    for (size_t i = 0; i < location_count; i++) {
        const IntermediateLocation *loc = &locations[i];
        if (loc->reducer_id != reducer_id) {
            continue;
        }
        int err = 0;
        if (loc->node_id == args->node_id) {
            err = append_file_range(spill, loc->path, loc->offset, loc->bytes);
        } else {
            err = shuffle_fetch_remote(args, loc, spill);
        }
        if (err != 0) {
            fclose(spill);
            return err;
        }
    }
    fclose(spill);
    trace->shuffle_end_ns = hydra_now_ns();

    trace->parse_sort_begin_ns = hydra_now_ns();
    size_t spill_len = 0;
    char *spill_data = read_file(spill_path, &spill_len);
    if (spill_data == NULL) {
        return errno ? errno : EIO;
    }

    mr_partitions input = {0};
    mr_partitions output = {0};
    mr_partitions_init(&input, 1);
    mr_partitions_init(&output, 1);
    int err = mr_partitions_parse(&input, spill_data, spill_len, false, 0);
    free(spill_data);
    if (err == 0) {
        mr_partitions_sort(&input);
        trace->parse_sort_end_ns = hydra_now_ns();
        current_reduce_input = &input;
        current_reduce_partition_number = (int)reducer_id;
        set_emit_context(EMIT_REDUCE, &output, MR_DefaultHashPartition);
        mr_partition *partition = &input.items[0];
        while (partition->cursor < partition->len) {
            char *key = partition->pairs[partition->cursor].key;
            if (trace->first_callback_entry_ns == 0) {
                trace->first_callback_entry_ns = hydra_now_ns();
            }
            reduce(key, reduce_get_next, (int)reducer_id);
            while (partition->cursor < partition->len &&
                   strcmp(partition->pairs[partition->cursor].key, key) == 0) {
                partition->cursor++;
            }
        }
        trace->callback_region_end_ns = hydra_now_ns();
        clear_emit_context();
        current_reduce_input = NULL;
        current_reduce_partition_number = 0;
        trace->serialize_begin_ns = hydra_now_ns();
        err = mr_partitions_serialize(&output, out, 0);
        trace->serialize_end_ns = hydra_now_ns();
    }
    mr_partitions_destroy(&output);
    mr_partitions_destroy(&input);
    (void)reducer_count;
    return err;
}

static int handle_shuffle_fetch(const mr_wire_header *hdr, const char *body,
                                size_t body_len, uint64_t payload_max,
                                byte_buf *response)
{
    if (hdr->kind != MR_TASK_SHUFFLE_FETCH ||
        body_len != sizeof(shuffle_fetch_request)) {
        return EINVAL;
    }

    shuffle_fetch_request req = {0};
    memcpy(&req, body, sizeof(req));
    FILE *fp = fopen(req.path, "rb");
    if (fp == NULL) {
        return errno;
    }
    if (fseeko(fp, (off_t)req.offset, SEEK_SET) != 0) {
        int err = errno;
        fclose(fp);
        return err;
    }

    size_t max_body = (size_t)(payload_max - sizeof(mr_wire_header));
    size_t max_payload = max_body > sizeof(ShuffleChunkHeader)
                             ? max_body - sizeof(ShuffleChunkHeader)
                             : 0;
    size_t want = req.bytes < max_payload ? (size_t)req.bytes : max_payload;
    byte_buf chunk_payload = {0};
    int err = 0;
    if (want > 0) {
        byte_buf_reserve(&chunk_payload, want);
        size_t got = fread(chunk_payload.data, 1, want, fp);
        chunk_payload.len = got;
        chunk_payload.data[got] = '\0';
        if (got != want) {
            err = ferror(fp) ? (errno ? errno : EIO) : EIO;
        }
    }
    fclose(fp);
    if (err != 0) {
        byte_buf_clear(&chunk_payload);
        return err;
    }

    ShuffleChunkHeader chunk = {
        .job_id = req.job_id,
        .mapper_id = 0,
        .reducer_id = req.reducer_id,
        .partition_seq = req.map_task_id,
        .chunk_seq = (uint32_t)hdr->task_id,
        .flags = SHUFFLE_CHUNK_FIRST | SHUFFLE_CHUNK_LAST,
        .record_count = 0,
        .payload_bytes = (uint32_t)chunk_payload.len,
    };
    byte_buf_append(response, &chunk, sizeof(chunk));
    if (chunk_payload.len > 0) {
        byte_buf_append(response, chunk_payload.data, chunk_payload.len);
    }
    byte_buf_clear(&chunk_payload);
    return 0;
}

static int porter_execute_task(void *context, uint32_t worker_id,
                               uint32_t kind, uint64_t task_id,
                               uint64_t partition_id,
                               uint64_t partition_count,
                               const MR_WorkerStateV1 *worker_state,
                               mr_template_binding_result *binding,
                               const void *payload, size_t payload_len,
                               byte_buf *out, mr_task_trace_v3 *trace)
{
    (void)task_id;
    (void)partition_id;
    (void)partition_count;
    worker_child_args args = *(const worker_child_args *)context;
    args.worker_id = worker_id;
    args.global_worker_id = args.global_worker_base + worker_id;
    (void)mkdir_p(args.store_dir);
    mr_worker_role expected_role =
        kind == MR_TASK_MAP ? MR_ROLE_MAP : MR_ROLE_REDUCE;
    if (kind == MR_TASK_ACTIVATION_PROBE &&
        payload_len >= sizeof(mr_activation_probe_request)) {
        const mr_activation_probe_request *probe = payload;
        expected_role = (mr_worker_role)probe->expected_role;
    }
    begin_worker_state(worker_state, expected_role, trace);
    int err = 0;

    if (kind == MR_TASK_MAP) {
        if (payload_len < sizeof(control_map_task)) {
            err = EINVAL;
            goto out;
        }
        control_map_task task = {0};
        memcpy(&task, payload, sizeof(task));
        const char *data = (const char *)payload + sizeof(task);
        if (sizeof(task) + task.data_len > payload_len) {
            err = EINVAL;
            goto out;
        }
        err = worker_do_map(&args, args.map, args.partitioner, data,
                            (size_t)task.data_len, task.job_id,
                            task.map_task_id, (int)task.reducer_count, out,
                            trace);
    } else if (kind == MR_TASK_REDUCE) {
        if (payload_len < sizeof(control_reduce_task)) {
            err = EINVAL;
            goto out;
        }
        control_reduce_task task = {0};
        memcpy(&task, payload, sizeof(task));
        size_t loc_bytes =
            (size_t)task.location_count * sizeof(IntermediateLocation);
        if (sizeof(task) + loc_bytes > payload_len) {
            err = EINVAL;
            goto out;
        }
        const IntermediateLocation *locs =
            (const IntermediateLocation *)((const char *)payload + sizeof(task));
        err = worker_do_reduce(&args, args.reduce, task.reducer_id,
                               task.reducer_count, locs,
                               (size_t)task.location_count, out, trace);
    } else if (kind == MR_TASK_ACTIVATION_PROBE) {
        if (payload_len != sizeof(mr_activation_probe_request)) {
            err = EINVAL;
            goto out;
        }
        const mr_activation_probe_request *probe = payload;
        if (probe->reserved0 != 0 || probe->reserved1[0] != 0 ||
            probe->reserved1[1] != 0 ||
            (probe->expected_role != MR_ROLE_MAP &&
             probe->expected_role != MR_ROLE_REDUCE)) {
            err = EINVAL;
            goto out;
        }
        trace->first_callback_entry_ns = hydra_now_ns();
        const MR_WorkerStateV1 *state = MR_GetWorkerState();
        if (state == NULL ||
            (probe->expected_state_checksum != 0 &&
             probe->expected_state_checksum != state->checksum)) {
            err = EILSEQ;
            goto out;
        }
        mr_activation_probe_response response = {
            .sequence = probe->sequence,
            .state_checksum = state->checksum,
            .proof = state->checksum ^ state->value_scale ^
                     ((uint64_t)state->role << 32) ^ probe->sequence,
            .access_count = 1,
        };
        byte_buf_append(out, &response, sizeof(response));
        trace->callback_region_end_ns = hydra_now_ns();
    } else {
        err = EINVAL;
    }
out:
    {
        int state_err = finish_worker_state(binding);
        return err != 0 ? err : state_err;
    }
}

static int agent_submit_request(mr_porter *porter, size_t worker_count,
                                mr_worker_role role, uint64_t agent_count,
                                const mr_wire_header *hdr, const char *body,
                                size_t body_len, uint32_t *worker_out,
                                uint64_t request_received_ns,
                                uint64_t parse_done_ns)
{
    uint32_t worker_index;
    if (hdr->kind == MR_TASK_MAP) {
        if (role == MR_ROLE_REDUCE) {
            return EOPNOTSUPP;
        }
        if (body_len < sizeof(control_map_task)) {
            return EINVAL;
        }
        control_map_task task = {0};
        memcpy(&task, body, sizeof(task));
        if (task.target_worker_id == MR_TARGET_WORKER_AUTO) {
            worker_index =
                (uint32_t)((hdr->task_id / agent_count) % worker_count);
        } else {
            worker_index = task.target_worker_id;
        }
        if (worker_index >= worker_count) {
            return EINVAL;
        }
    } else if (hdr->kind == MR_TASK_REDUCE) {
        if (role == MR_ROLE_MAP) {
            return EOPNOTSUPP;
        }
        if (body_len < sizeof(control_reduce_task)) {
            return EINVAL;
        }
        control_reduce_task task = {0};
        memcpy(&task, body, sizeof(task));
        if (task.target_worker_id == MR_TARGET_WORKER_AUTO) {
            worker_index =
                (uint32_t)((hdr->partition_id / agent_count) % worker_count);
        } else {
            worker_index = task.target_worker_id;
        }
        if (worker_index >= worker_count) {
            return EINVAL;
        }
    } else if (hdr->kind == MR_TASK_ACTIVATION_PROBE) {
        if (body_len != sizeof(mr_activation_probe_request)) {
            return EINVAL;
        }
        mr_activation_probe_request probe = {0};
        memcpy(&probe, body, sizeof(probe));
        if ((probe.expected_role != MR_ROLE_MAP &&
             probe.expected_role != MR_ROLE_REDUCE) ||
            (role != MR_ROLE_BOTH && role != (mr_worker_role)probe.expected_role)) {
            return EACCES;
        }
        worker_index = probe.target_worker_id == MR_TARGET_WORKER_AUTO ?
            (uint32_t)(probe.sequence % worker_count) :
            probe.target_worker_id;
        if (worker_index >= worker_count) {
            return EINVAL;
        }
    } else {
        return EINVAL;
    }
    int err = mr_porter_submit(porter, worker_index, hdr->kind, hdr->task_id,
                               hdr->partition_id, hdr->partition_count, body,
                               body_len, request_received_ns, parse_done_ns);
    if (err == 0 && worker_out != NULL) {
        *worker_out = worker_index;
    }
    return err;
}

static mr_kernel_restore_stats_v3 kernel_stats_from_restore(
    const mr_cxlfork_result *restore)
{
    mr_kernel_restore_stats_v3 stats = {
        .supported_mask = restore->exclusive_supported_mask,
        .valid_mask = restore->exclusive_valid_mask,
        .path_mask = restore->path_mask,
        .total_ns = restore->kernel_total_ns,
        .lock_wait_ns = restore->lock_wait_ns,
        .lifecycle_ns = restore->lifecycle_ns,
        .manifest_validate_ns = restore->manifest_validate_ns,
        .image_validate_ns = restore->image_validate_ns,
        .checksum_validate_ns = restore->checksum_validate_ns,
        .address_conflict_ns = restore->address_conflict_ns,
        .vma_create_ns = restore->vma_create_ns,
        .upper_pt_alloc_ns = restore->upper_pt_alloc_ns,
        .shared_leaf_attach_ns = restore->shared_leaf_attach_exclusive_ns,
        .pte_image_validate_ns = restore->pte_image_validate_ns,
        .direct_pte_install_ns = restore->direct_pte_install_ns,
        .fallback_other_ns = restore->fallback_other_ns,
        .dirty_prefault_ns = restore->dirty_prefault_ns,
        .global_restore_ns = restore->global_restore_ns,
        .other_ns = restore->kernel_other_ns,
        .faults = restore->faults,
        .fault_dispatch_ns = restore->fault_dispatch_ns,
        .leaf_materialize_ns = restore->leaf_materialize_ns,
        .data_cow_ns = restore->data_cow_ns,
        .cxl_to_local_copy_ns = restore->cxl_to_local_copy_ns,
        .shared_leaf_count = restore->shared_leaf_count,
        .shared_data_pages = restore->shared_data_pages,
        .fallback_pages = restore->fallback_pages,
        .direct_pte_install_pages = restore->direct_pte_install_pages,
        .local_copy_pages = restore->local_copy_pages,
        .leaf_copy_pages = restore->leaf_copy_pages,
        .dirty_prefault_pages = restore->dirty_prefault_pages,
        .validated_ptes = restore->validated_ptes,
    };

    return stats;
}

static int run_agent(const agent_args *args, Mapper map, Reducer reduce,
                     Partitioner partitioner)
{
    uint64_t agent_process_start_ns = hydra_now_ns();
    uint64_t agent_transport_ready_ns = 0;
    uint64_t agent_ready_published_ns = 0;
    mr_porter_start_trace_v1 porter_start_trace = {0};
    mr_porter_stop_trace_v1 porter_stop_trace = {0};
    int err = mkdir_p(args->store_dir);
    if (err != 0) {
        fprintf(stderr, "agent: create store failed: %s\n", strerror(err));
        return 1;
    }

    worker_child_args worker_context = {
        .map = map,
        .reduce = reduce,
        .partitioner = partitioner,
        .shuffle_backend = args->shuffle_backend,
        .node_id = (uint32_t)args->node_id,
        .global_worker_base = (uint32_t)args->global_worker_base,
        .map_size = args->shuffle_map_size,
        .queue_capacity = args->queue_capacity,
        .payload_max = args->payload_max,
        .shuffle_connections = args->shuffle_connections,
        .shuffle_queue_policy = args->shuffle_queue_policy,
    };
    snprintf(worker_context.store_dir, sizeof(worker_context.store_dir), "%s",
             args->store_dir);
    snprintf(worker_context.shuffle_path, sizeof(worker_context.shuffle_path),
             "%s", args->path);

    mr_porter_config porter_config = {
        .mode = args->launch_mode,
        .workers = (uint32_t)args->workers,
        .slot_bytes = args->payload_max,
        .template = {
            .role = args->role,
            .key = args->template_key,
            .base = args->template_base,
            .bytes = args->template_bytes,
            .seed = args->template_seed,
            .value_scale = args->template_value_scale,
        },
        .restore = {
            .template = {
                .role = args->role,
                .key = args->template_key,
                .base = args->template_base,
                .bytes = args->template_bytes,
                .seed = args->template_seed,
                .value_scale = args->template_value_scale,
            },
            .generation = args->template_generation,
            .page_checksum = args->template_page_checksum,
            .policy = args->cxlfork_policy,
            .validate_full = args->validate_full,
        },
        .restore_hold_ms = (uint32_t)args->restore_hold_ms,
        .restore_preopen = args->restore_preopen,
        .task_fn = porter_execute_task,
        .task_context = &worker_context,
    };
    mr_porter *porter = NULL;

    hydra_ctx *ctx = NULL;
    mr_hydra_open_config config = {
        .backend = args->backend,
        .path = args->path,
        .map_size = args->map_size,
        .queue_capacity = args->queue_capacity,
        .payload_max = args->payload_max,
        .connections = args->shuffle_connections,
        .connection_id = 0,
        .node_id = 0,
        .peer_node_id = args->node_id + 1,
        .service_id = MR_SERVICE_CONTROL,
        .queue_policy = args->shuffle_queue_policy,
        .create = args->create,
    };
    for (uint32_t attempt = 0; attempt <= MR_AGENT_OPEN_RETRY_COUNT; attempt++) {
        err = mr_hydra_open_ex(&ctx, &config);
        if (err == 0 || args->create) {
            break;
        }
        usleep(MR_AGENT_OPEN_RETRY_US);
    }
    if (err != 0) {
        fprintf(stderr, "agent: open control backend failed: %s\n",
                hydra_strerror(err));
        return 1;
    }
    agent_transport_ready_ns = hydra_now_ns();

    if (!args->lazy_porter) {
        err = mr_porter_start_timed(&porter_config, &porter,
                                    &porter_start_trace);
        if (err != 0) {
            fprintf(stderr, "agent: start porter failed: %s\n",
                    strerror(err));
            hydra_close(ctx);
            return 1;
        }
    }

    if (args->ready_file != NULL) {
        int ready_fd = open(args->ready_file,
                            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (ready_fd < 0 || close(ready_fd) != 0) {
            int ready_err = errno;
            fprintf(stderr, "agent: create ready file failed: %s\n",
                    strerror(ready_err));
            hydra_close(ctx);
            mr_porter_stop(porter);
            return 1;
        }
    }
    agent_ready_published_ns = hydra_now_ns();

    if (!args->quiet) {
        printf("agent[%" PRIu64 "]: role=%s launch=%s workers=%" PRIu64
               " porter_start=%s endpoint=%s store=%s\n",
               args->node_id, mr_role_name(args->role),
               mr_launch_mode_name(args->launch_mode), args->workers,
               args->lazy_porter ? "lazy" : "eager",
               args->path, args->store_dir);
        fflush(stdout);
    }

    typedef struct agent_pending {
        bool active;
        bool lifecycle_cold;
        hydra_msg request;
        mr_wire_header header;
        mr_porter_start_trace_v1 cold_start;
    } agent_pending;
    agent_pending *pending =
        mr_xcalloc((size_t)args->workers, sizeof(*pending));
    size_t active_count = 0;
    bool stop_requested = false;
    while (!stop_requested || active_count != 0) {
        bool progress = false;
        uint64_t offset = 0;
        const hydra_msg *msg = NULL;
        const uint8_t *payload = NULL;
        err = hydra_recv_request(ctx, &offset, &msg, &payload);
        if (err == 0) {
            progress = true;
            uint64_t agent_received_ns = hydra_now_ns();
            hydra_msg request = *msg;
            request.server_recv_ns = agent_received_ns;
            mr_wire_header hdr = {0};
            const char *body = NULL;
            size_t body_len = 0;
            byte_buf response = {0};
            uint32_t status = 0;
            int request_err =
                mr_wire_parse(payload, msg->len, &hdr, &body, &body_len);
            uint64_t parse_done_ns = hydra_now_ns();
            request.server_dispatch_ns = parse_done_ns - agent_received_ns;
            bool deferred = false;

            if (request_err == 0 &&
                (hdr.kind == MR_TASK_MAP || hdr.kind == MR_TASK_REDUCE ||
                 hdr.kind == MR_TASK_ACTIVATION_PROBE)) {
                bool lifecycle_cold = false;
                mr_porter_start_trace_v1 cold_start = {0};
                if (porter == NULL) {
                    if (!args->lazy_porter) {
                        request_err = EPROTO;
                    } else {
                        request_err = mr_porter_start_timed(
                            &porter_config, &porter, &cold_start);
                        if (request_err == 0) {
                            porter_start_trace = cold_start;
                            lifecycle_cold = true;
                        }
                    }
                }
                uint32_t worker = UINT32_MAX;
                if (request_err == 0) {
                    request_err = agent_submit_request(
                        porter, (size_t)args->workers, args->role,
                        args->agent_count, &hdr, body, body_len, &worker,
                        agent_received_ns, parse_done_ns);
                }
                if (request_err == 0) {
                    if (pending[worker].active) {
                        request_err = EBUSY;
                    } else {
                        pending[worker].active = true;
                        pending[worker].lifecycle_cold = lifecycle_cold;
                        pending[worker].request = request;
                        pending[worker].header = hdr;
                        pending[worker].cold_start = cold_start;
                        active_count++;
                        deferred = true;
                    }
                }
            } else if (request_err == 0 &&
                       hdr.kind == MR_TASK_SHUFFLE_FETCH) {
                request_err = handle_shuffle_fetch(
                    &hdr, body, body_len, args->payload_max, &response);
            } else if (request_err == 0 && hdr.kind == MR_TASK_SHUTDOWN) {
                stop_requested = true;
            } else if (request_err == 0) {
                request_err = EINVAL;
            }

            (void)hydra_release_message(ctx, offset);
            if (!deferred) {
                if (request_err != 0) {
                    status = 1;
                    byte_buf_clear(&response);
                    const char *text = "agent task failed\n";
                    byte_buf_append(&response, text, strlen(text));
                }
                byte_buf wire = make_wire(
                    request_err == 0 ? (mr_task_kind)hdr.kind : MR_TASK_ERROR,
                    hdr.task_id, hdr.partition_id, hdr.partition_count,
                    response.data, response.len);
                request.server_stack_ns = hydra_now_ns() - agent_received_ns;
                mr_hydra_send_observation ignored_send = {0};
                (void)mr_hydra_send_response_poll_observed(
                    ctx, &request, status, &wire, &ignored_send);
                byte_buf_clear(&wire);
                byte_buf_clear(&response);
            }
        } else if (err != EAGAIN) {
            fprintf(stderr, "agent: receive request failed: %s\n",
                    hydra_strerror(err));
            break;
        }

        while (porter != NULL) {
            uint32_t worker = UINT32_MAX;
            byte_buf response = {0};
            mr_porter_result porter_result = {0};
            int collect_err =
                mr_porter_collect(porter, &worker, &response, &porter_result);
            if (collect_err == EAGAIN || collect_err == EWOULDBLOCK) {
                byte_buf_clear(&response);
                break;
            }
            progress = true;
            if (worker >= args->workers || !pending[worker].active) {
                byte_buf_clear(&response);
                fprintf(stderr, "agent: unmatched porter completion\n");
                stop_requested = true;
                break;
            }
            agent_pending completed = pending[worker];
            pending[worker].active = false;
            active_count--;

            if (!args->quiet) {
                printf("porter_task_v1 task=%" PRIu64 " kind=%u node=%" PRIu64
                       " worker=%u pid=%" PRId64 " launch=%s status=%u"
                       " total_ns=%" PRIu64 " fork_ns=%" PRIu64
                       " restore_ioctl_ns=%" PRIu64 " task_ns=%" PRIu64
                       " refill_ns=%" PRIu64 " restore_id=%" PRIu64
                       " path_mask=%" PRIu64 " kernel_total_ns=%" PRIu64
                       " shared_leaf_pages=%" PRIu64
                       " fallback_pages=%" PRIu64
                       " local_copy_pages=%" PRIu64
                       " template_state_checksum=%" PRIu64
                       " template_access_checksum=%" PRIu64
                       " template_access_count=%" PRIu64
                       " template_state_pages=%" PRIu64
                       " template_binding_flags=%" PRIu64 "\n",
                       completed.header.task_id, completed.header.kind,
                       args->node_id, porter_result.worker_id,
                       porter_result.worker_pid,
                       mr_launch_mode_name(args->launch_mode),
                       porter_result.status, porter_result.total_ns,
                       porter_result.fork_ns,
                       porter_result.restore_ioctl_ns,
                       porter_result.task_ns, porter_result.refill_ns,
                       porter_result.restore.restore_id,
                       porter_result.restore.path_mask,
                       porter_result.restore.kernel_total_ns,
                       porter_result.restore.shared_leaf_pages,
                       porter_result.restore.fallback_pages,
                       porter_result.restore.local_copy_pages,
                       porter_result.binding.state_checksum,
                       porter_result.binding.access_checksum,
                       porter_result.binding.access_count,
                       porter_result.binding.state_pages,
                       porter_result.binding.flags);
                fflush(stdout);
            }
            porter_result.trace.agent_collect_done_ns = hydra_now_ns();
            uint32_t status = collect_err == 0 ? 0 : 1;
            if (collect_err != 0) {
                byte_buf_clear(&response);
                const char *text = "agent task failed\n";
                byte_buf_append(&response, text, strlen(text));
            }
            mr_task_result_v4 task_result = {
                .magic = MR_TASK_RESULT_MAGIC,
                .size = sizeof(task_result),
                .version = MR_TASK_RESULT_VERSION,
                .kind = completed.header.kind,
                .status = (uint32_t)(collect_err == 0 ? 0 : collect_err),
                .node_id = (uint32_t)args->node_id,
                .worker_id = porter_result.worker_id,
                .task_id = completed.header.task_id,
                .worker_pid = porter_result.worker_pid,
                .launch_mode = args->launch_mode,
                .policy = args->cxlfork_policy,
                .validate_full = args->validate_full ? 1U : 0U,
                .trace_flags = porter_result.trace_flags |
                    (completed.lifecycle_cold ?
                         MR_TRACE_F_LIFECYCLE_COLD : 0U),
                .trace = porter_result.trace,
                .restore_wrapper_ns =
                    porter_result.restore_timing.wrapper_total_ns,
                .session_alloc_ns =
                    porter_result.restore_timing.session_alloc_ns,
                .device_open_ns =
                    porter_result.restore_timing.device_open_ns,
                .query_caps_ns =
                    porter_result.restore_timing.query_caps_ns,
                .policy_check_ns =
                    porter_result.restore_timing.policy_check_ns,
                .resume_ioctl_ns =
                    porter_result.restore_timing.resume_ioctl_ns,
                .restore_postcheck_ns =
                    porter_result.restore_timing.postcheck_ns,
                .preopen_device_ns =
                    porter_result.restore_timing.preopen_device_ns,
                .preopen_caps_ns =
                    porter_result.restore_timing.preopen_caps_ns,
                .preopen_policy_ns =
                    porter_result.restore_timing.preopen_policy_ns,
                .restore_setup_mode =
                    porter_result.restore_timing.setup_mode,
                .kernel = kernel_stats_from_restore(&porter_result.restore),
                .template_key = args->template_key,
                .template_generation = args->template_generation,
                .restore_id = porter_result.restore.restore_id,
                .restore_path_mask = porter_result.restore.path_mask,
                .total_pages = porter_result.restore.total_pages,
                .page_checksum = porter_result.restore.page_checksum,
                .template_state_checksum =
                    porter_result.binding.state_checksum,
                .template_access_checksum =
                    porter_result.binding.access_checksum,
                .template_access_count = porter_result.binding.access_count,
                .template_state_pages = porter_result.binding.state_pages,
                .template_binding_flags = porter_result.binding.flags,
                .cold_start = completed.cold_start,
                .executed_worker = porter_result.executed_worker,
                .replacement_worker = porter_result.replacement_worker,
            };
            byte_buf measured = {0};
            byte_buf_append(&measured, &task_result, sizeof(task_result));
            byte_buf_append(&measured, response.data, response.len);
            task_result.trace.response_encode_done_ns = hydra_now_ns();
            memcpy(measured.data, &task_result, sizeof(task_result));
            byte_buf_clear(&response);
            response = measured;
            byte_buf wire = make_wire(
                (mr_task_kind)completed.header.kind,
                completed.header.task_id, completed.header.partition_id,
                completed.header.partition_count, response.data,
                response.len);
            completed.request.server_stack_ns =
                hydra_now_ns() - completed.request.server_recv_ns;
            mr_hydra_send_observation response_send = {0};
            (void)mr_hydra_send_response_poll_observed(
                ctx, &completed.request, status, &wire, &response_send);
            fprintf(stderr,
                    "hydra_response_v1 task=%" PRIu64
                    " call_begin_ns=%" PRIu64
                    " publish_done_ns=%" PRIu64
                    " queue_ns=%" PRIu64 " alloc_ns=%" PRIu64
                    " header_ns=%" PRIu64 " copy_ns=%" PRIu64
                    " checksum_ns=%" PRIu64 " visibility_ns=%" PRIu64
                    " publish_ns=%" PRIu64 " wait_ns=%" PRIu64
                    " retries=%" PRIu64 " queue_empty=%u\n",
                    completed.header.task_id, response_send.call_begin_ns,
                    response_send.publish_done_ns,
                    response_send.phases.queue_reserve_ns,
                    response_send.phases.alloc_ns,
                    response_send.phases.header_ns,
                    response_send.phases.payload_copy_ns,
                    response_send.phases.checksum_ns,
                    response_send.phases.visibility_ns,
                    response_send.phases.publish_ns, response_send.wait_ns,
                    response_send.retries, response_send.queue_was_empty);
            byte_buf_clear(&wire);
            byte_buf_clear(&response);
        }
        if (!progress) {
            hydra_cpu_relax();
        }
    }

    free(pending);
    hydra_close(ctx);
    mr_porter_stop_timed(porter, &porter_stop_trace);
    fprintf(stderr,
            "porter_lifecycle_v1 node=%" PRIu64 " role=%u lazy=%u"
            " workers=%" PRIu64
            " agent_process_start_ns=%" PRIu64
            " transport_ready_ns=%" PRIu64
            " agent_ready_ns=%" PRIu64
            " porter_start_begin_ns=%" PRIu64
            " shared_map_begin_ns=%" PRIu64
            " shared_map_end_ns=%" PRIu64
            " socketpair_begin_ns=%" PRIu64
            " socketpair_end_ns=%" PRIu64
            " supervisor_fork_begin_ns=%" PRIu64
            " supervisor_fork_return_ns=%" PRIu64
            " supervisor_entry_ns=%" PRIu64
            " pool_spawn_begin_ns=%" PRIu64
            " first_worker_ready_ns=%" PRIu64
            " initial_pool_ready_ns=%" PRIu64
            " porter_init_reply_received_ns=%" PRIu64
            " porter_start_end_ns=%" PRIu64
            " stop_begin_ns=%" PRIu64
            " stop_command_sent_ns=%" PRIu64
            " supervisor_stop_received_ns=%" PRIu64
            " workers_stop_begin_ns=%" PRIu64
            " workers_stop_end_ns=%" PRIu64
            " stop_reply_received_ns=%" PRIu64
            " supervisor_wait_begin_ns=%" PRIu64
            " supervisor_wait_end_ns=%" PRIu64
            " stop_end_ns=%" PRIu64 "\n",
            args->node_id, (unsigned int)args->role,
            args->lazy_porter ? 1U : 0U, args->workers,
            agent_process_start_ns, agent_transport_ready_ns,
            agent_ready_published_ns, porter_start_trace.start_begin_ns,
            porter_start_trace.shared_map_begin_ns,
            porter_start_trace.shared_map_end_ns,
            porter_start_trace.socketpair_begin_ns,
            porter_start_trace.socketpair_end_ns,
            porter_start_trace.supervisor_fork_begin_ns,
            porter_start_trace.supervisor_fork_return_ns,
            porter_start_trace.supervisor_entry_ns,
            porter_start_trace.pool_spawn_begin_ns,
            porter_start_trace.first_worker_ready_ns,
            porter_start_trace.pool_ready_ns,
            porter_start_trace.init_reply_received_ns,
            porter_start_trace.start_end_ns,
            porter_stop_trace.stop_begin_ns,
            porter_stop_trace.stop_command_sent_ns,
            porter_stop_trace.supervisor_stop_received_ns,
            porter_stop_trace.workers_stop_begin_ns,
            porter_stop_trace.workers_stop_end_ns,
            porter_stop_trace.stop_reply_received_ns,
            porter_stop_trace.supervisor_wait_begin_ns,
            porter_stop_trace.supervisor_wait_end_ns,
            porter_stop_trace.stop_end_ns);
    return 0;
}

static int open_agent_endpoint(mr_endpoint *ep, const master_args *args,
                               size_t index)
{
    mr_hydra_open_config config = {
        .backend = args->backend,
        .path = ep->path,
        .map_size = args->map_size,
        .queue_capacity = args->queue_capacity,
        .payload_max = args->payload_max,
        .connections = args->connections,
        .connection_id = 0,
        .node_id = 0,
        .peer_node_id = index + 1,
        .service_id = MR_SERVICE_CONTROL,
        .queue_policy = args->queue_policy,
        .create = true,
    };
    return mr_hydra_open_ex(&ep->ctx, &config);
}

static int unwrap_task_result(byte_buf *response, mr_task_result_v4 *result)
{
    struct {
        uint32_t magic;
        uint16_t size;
        uint16_t version;
    } header;

    if (response->len < sizeof(header)) {
        return EPROTO;
    }
    memcpy(&header, response->data, sizeof(header));
    if (header.magic != MR_TASK_RESULT_MAGIC ||
        header.version != MR_TASK_RESULT_VERSION ||
        header.size != sizeof(*result) || response->len < sizeof(*result)) {
        return EPROTO;
    }
    memcpy(result, response->data, sizeof(*result));
    if (result->magic != MR_TASK_RESULT_MAGIC ||
        result->size != sizeof(*result) ||
        result->version != MR_TASK_RESULT_VERSION) {
        return EPROTO;
    }
    for (size_t i = 0; i < sizeof(result->reserved) /
                               sizeof(result->reserved[0]);
         i++) {
        if (result->reserved[i] != 0) {
            return EPROTO;
        }
    }
    size_t business_len = response->len - sizeof(*result);
    if (business_len > 0) {
        memmove(response->data, response->data + sizeof(*result),
                business_len);
    }
    response->len = business_len;
    if (response->data != NULL) {
        response->data[business_len] = '\0';
    }
    return result->status == 0 ? 0 : EREMOTEIO;
}

static void report_task_result_observed(
    const mr_task_result_v4 *result, uint64_t master_roundtrip_ns,
    const mr_hydra_send_observation *request_send,
    const hydra_msg *response_msg, uint64_t response_recv_ns,
    uint64_t response_release_done_ns)
{
    fprintf(stderr,
            "task_result_v4 task=%" PRIu64 " kind=%u node=%u worker=%u"
            " pid=%" PRId64 " status=%u roundtrip_ns=%" PRIu64
            " launch=%u policy=%u validation=%u trace_flags=%u"
            " agent_recv_ns=%" PRIu64 " parse_done_ns=%" PRIu64
            " porter_submit_ns=%" PRIu64 " payload_done_ns=%" PRIu64
            " slot_publish_ns=%" PRIu64 " supervisor_recv_ns=%" PRIu64
            " fork_begin_ns=%" PRIu64 " fork_return_ns=%" PRIu64
            " worker_observe_ns=%" PRIu64 " worker_entry_ns=%" PRIu64
            " restore_begin_ns=%" PRIu64 " restore_end_ns=%" PRIu64
            " resume_begin_ns=%" PRIu64 " resume_end_ns=%" PRIu64
            " restore_postcheck_done_ns=%" PRIu64
            " bind_begin_ns=%" PRIu64 " bind_end_ns=%" PRIu64
            " state_ready_ns=%" PRIu64 " business_entry_ns=%" PRIu64
            " first_state_access_ns=%" PRIu64
            " first_callback_ns=%" PRIu64 " callback_end_ns=%" PRIu64
            " shuffle_begin_ns=%" PRIu64 " shuffle_end_ns=%" PRIu64
            " parse_sort_begin_ns=%" PRIu64 " parse_sort_end_ns=%" PRIu64
            " serialize_begin_ns=%" PRIu64 " serialize_end_ns=%" PRIu64
            " task_done_ns=%" PRIu64 " collect_begin_ns=%" PRIu64
            " collect_end_ns=%" PRIu64 " slot_done_ns=%" PRIu64
            " supervisor_done_ns=%" PRIu64 " reap_begin_ns=%" PRIu64
            " reap_end_ns=%" PRIu64 " refill_begin_ns=%" PRIu64
            " refill_end_ns=%" PRIu64 " pool_ready_ns=%" PRIu64
            " agent_collect_ns=%" PRIu64
            " response_encode_done_ns=%" PRIu64
            " restore_wrapper_ns=%" PRIu64
            " session_alloc_ns=%" PRIu64 " device_open_ns=%" PRIu64
            " query_caps_ns=%" PRIu64 " policy_check_ns=%" PRIu64
            " resume_ioctl_ns=%" PRIu64 " restore_postcheck_ns=%" PRIu64
            " preopen_device_ns=%" PRIu64 " preopen_caps_ns=%" PRIu64
            " preopen_policy_ns=%" PRIu64 " restore_setup=%" PRIu64
            " restore_id=%" PRIu64 " path_mask=%" PRIu64
            " template_key=%" PRIu64 " template_generation=%" PRIu64
            " total_pages=%" PRIu64 " page_checksum=%" PRIu64
            " kernel_supported_mask=%" PRIu64
            " kernel_valid_mask=%" PRIu64
            " kernel_total_ns=%" PRIu64
            " kernel_lock_ns=%" PRIu64 " kernel_lifecycle_ns=%" PRIu64
            " kernel_manifest_ns=%" PRIu64 " kernel_image_ns=%" PRIu64
            " kernel_checksum_ns=%" PRIu64 " kernel_conflict_ns=%" PRIu64
            " kernel_vma_ns=%" PRIu64 " kernel_upper_pt_ns=%" PRIu64
            " kernel_shared_leaf_ns=%" PRIu64
            " kernel_pte_validate_ns=%" PRIu64
            " kernel_pte_install_ns=%" PRIu64
            " kernel_fallback_other_ns=%" PRIu64
            " kernel_dirty_prefault_ns=%" PRIu64
            " kernel_global_ns=%" PRIu64 " kernel_other_ns=%" PRIu64
            " faults=%" PRIu64 " fault_dispatch_ns=%" PRIu64
            " leaf_materialize_ns=%" PRIu64 " data_cow_ns=%" PRIu64
            " cxl_copy_ns=%" PRIu64 " shared_leaf_count=%" PRIu64
            " shared_data_pages=%" PRIu64 " fallback_pages=%" PRIu64
            " direct_pte_install_pages=%" PRIu64
            " local_copy_pages=%" PRIu64 " leaf_copy_pages=%" PRIu64
            " dirty_prefault_pages=%" PRIu64 " validated_ptes=%" PRIu64
            " template_state_checksum=%" PRIu64
            " template_access_checksum=%" PRIu64
            " template_access_count=%" PRIu64
            " template_state_pages=%" PRIu64
            " template_binding_flags=%" PRIu64 "\n",
            result->task_id, result->kind, result->node_id,
            result->worker_id, result->worker_pid, result->status,
            master_roundtrip_ns, result->launch_mode, result->policy,
            result->validate_full, result->trace_flags,
            result->trace.agent_request_received_ns,
            result->trace.agent_parse_done_ns,
            result->trace.porter_submit_begin_ns,
            result->trace.payload_copy_done_ns,
            result->trace.slot_ready_publish_ns,
            result->trace.supervisor_command_received_ns,
            result->trace.fork_begin_ns, result->trace.fork_return_ns,
            result->trace.worker_slot_observed_ns,
            result->trace.worker_entry_ns,
            result->trace.restore_wrapper_begin_ns,
            result->trace.restore_wrapper_end_ns,
            result->trace.resume_ioctl_begin_ns,
            result->trace.resume_ioctl_end_ns,
            result->trace.restore_postcheck_done_ns,
            result->trace.template_bind_begin_ns,
            result->trace.template_bind_end_ns,
            result->trace.state_ready_ns, result->trace.business_entry_ns,
            result->trace.first_state_access_ns,
            result->trace.first_callback_entry_ns,
            result->trace.callback_region_end_ns,
            result->trace.shuffle_begin_ns, result->trace.shuffle_end_ns,
            result->trace.parse_sort_begin_ns,
            result->trace.parse_sort_end_ns,
            result->trace.serialize_begin_ns,
            result->trace.serialize_end_ns, result->trace.task_done_ns,
            result->trace.restore_collect_begin_ns,
            result->trace.restore_collect_end_ns,
            result->trace.slot_done_publish_ns,
            result->trace.supervisor_done_observed_ns,
            result->trace.reap_begin_ns, result->trace.reap_end_ns,
            result->trace.refill_begin_ns, result->trace.refill_end_ns,
            result->trace.pool_ready_ns, result->trace.agent_collect_done_ns,
            result->trace.response_encode_done_ns,
            result->restore_wrapper_ns, result->session_alloc_ns,
            result->device_open_ns, result->query_caps_ns,
            result->policy_check_ns, result->resume_ioctl_ns,
            result->restore_postcheck_ns, result->preopen_device_ns,
            result->preopen_caps_ns, result->preopen_policy_ns,
            result->restore_setup_mode,
            result->restore_id, result->restore_path_mask,
            result->template_key, result->template_generation,
            result->total_pages, result->page_checksum,
            result->kernel.supported_mask, result->kernel.valid_mask,
            result->kernel.total_ns, result->kernel.lock_wait_ns,
            result->kernel.lifecycle_ns,
            result->kernel.manifest_validate_ns,
            result->kernel.image_validate_ns,
            result->kernel.checksum_validate_ns,
            result->kernel.address_conflict_ns,
            result->kernel.vma_create_ns,
            result->kernel.upper_pt_alloc_ns,
            result->kernel.shared_leaf_attach_ns,
            result->kernel.pte_image_validate_ns,
            result->kernel.direct_pte_install_ns,
            result->kernel.fallback_other_ns,
            result->kernel.dirty_prefault_ns,
            result->kernel.global_restore_ns, result->kernel.other_ns,
            result->kernel.faults, result->kernel.fault_dispatch_ns,
            result->kernel.leaf_materialize_ns,
            result->kernel.data_cow_ns,
            result->kernel.cxl_to_local_copy_ns,
            result->kernel.shared_leaf_count,
            result->kernel.shared_data_pages,
            result->kernel.fallback_pages,
            result->kernel.direct_pte_install_pages,
            result->kernel.local_copy_pages,
            result->kernel.leaf_copy_pages,
            result->kernel.dirty_prefault_pages,
            result->kernel.validated_ptes,
            result->template_state_checksum,
            result->template_access_checksum,
            result->template_access_count, result->template_state_pages,
            result->template_binding_flags);

    fprintf(stderr,
            "lifecycle_task_v1 task=%" PRIu64 " cold=%u"
            " porter_start_begin_ns=%" PRIu64
            " shared_map_begin_ns=%" PRIu64
            " shared_map_end_ns=%" PRIu64
            " socketpair_begin_ns=%" PRIu64
            " socketpair_end_ns=%" PRIu64
            " supervisor_fork_begin_ns=%" PRIu64
            " supervisor_fork_return_ns=%" PRIu64
            " supervisor_entry_ns=%" PRIu64
            " pool_spawn_begin_ns=%" PRIu64
            " first_worker_ready_ns=%" PRIu64
            " initial_pool_ready_ns=%" PRIu64
            " porter_init_reply_received_ns=%" PRIu64
            " porter_start_end_ns=%" PRIu64
            " executed_pid=%" PRId64
            " executed_spawn_flags=%u"
            " executed_fork_begin_ns=%" PRIu64
            " executed_fork_return_ns=%" PRIu64
            " executed_child_entry_ns=%" PRIu64
            " executed_device_open_ns=%" PRIu64
            " executed_query_caps_ns=%" PRIu64
            " executed_policy_ns=%" PRIu64
            " executed_ready_ns=%" PRIu64
            " replacement_pid=%" PRId64
            " replacement_spawn_flags=%u"
            " replacement_fork_begin_ns=%" PRIu64
            " replacement_fork_return_ns=%" PRIu64
            " replacement_child_entry_ns=%" PRIu64
            " replacement_device_open_ns=%" PRIu64
            " replacement_query_caps_ns=%" PRIu64
            " replacement_policy_ns=%" PRIu64
            " replacement_ready_ns=%" PRIu64 "\n",
            result->task_id,
            (result->trace_flags & MR_TRACE_F_LIFECYCLE_COLD) != 0,
            result->cold_start.start_begin_ns,
            result->cold_start.shared_map_begin_ns,
            result->cold_start.shared_map_end_ns,
            result->cold_start.socketpair_begin_ns,
            result->cold_start.socketpair_end_ns,
            result->cold_start.supervisor_fork_begin_ns,
            result->cold_start.supervisor_fork_return_ns,
            result->cold_start.supervisor_entry_ns,
            result->cold_start.pool_spawn_begin_ns,
            result->cold_start.first_worker_ready_ns,
            result->cold_start.pool_ready_ns,
            result->cold_start.init_reply_received_ns,
            result->cold_start.start_end_ns,
            result->executed_worker.pid,
            result->executed_worker.flags,
            result->executed_worker.fork_begin_ns,
            result->executed_worker.fork_return_ns,
            result->executed_worker.child_entry_ns,
            result->executed_worker.device_open_ns,
            result->executed_worker.query_caps_ns,
            result->executed_worker.policy_check_ns,
            result->executed_worker.ready_ns,
            result->replacement_worker.pid,
            result->replacement_worker.flags,
            result->replacement_worker.fork_begin_ns,
            result->replacement_worker.fork_return_ns,
            result->replacement_worker.child_entry_ns,
            result->replacement_worker.device_open_ns,
            result->replacement_worker.query_caps_ns,
            result->replacement_worker.policy_check_ns,
            result->replacement_worker.ready_ns);

    if (request_send != NULL && response_msg != NULL) {
        fprintf(stderr,
                "hydra_task_v1 task=%" PRIu64
                " request_call_begin_ns=%" PRIu64
                " request_publish_done_ns=%" PRIu64
                " request_queue_ns=%" PRIu64
                " request_alloc_ns=%" PRIu64
                " request_header_ns=%" PRIu64
                " request_copy_ns=%" PRIu64
                " request_checksum_ns=%" PRIu64
                " request_visibility_ns=%" PRIu64
                " request_publish_ns=%" PRIu64
                " request_wait_ns=%" PRIu64 " request_retries=%" PRIu64
                " request_queue_empty=%u response_recv_ns=%" PRIu64
                " response_release_done_ns=%" PRIu64
                " request_timestamp_ns=%" PRIu64
                " request_ready_ns=%" PRIu64
                " server_recv_ns=%" PRIu64
                " server_dispatch_ns=%" PRIu64
                " server_stack_ns=%" PRIu64
                " request_verify_ns=%" PRIu64
                " response_queue_ns=%" PRIu64
                " response_alloc_ns=%" PRIu64
                " response_fill_ns=%" PRIu64
                " response_checksum_ns=%" PRIu64
                " response_visibility_ns=%" PRIu64
                " response_placement_ns=%" PRIu64
                " response_ready_ns=%" PRIu64 "\n",
                result->task_id, request_send->call_begin_ns,
                request_send->publish_done_ns,
                request_send->phases.queue_reserve_ns,
                request_send->phases.alloc_ns,
                request_send->phases.header_ns,
                request_send->phases.payload_copy_ns,
                request_send->phases.checksum_ns,
                request_send->phases.visibility_ns,
                request_send->phases.publish_ns, request_send->wait_ns,
                request_send->retries, request_send->queue_was_empty,
                response_recv_ns, response_release_done_ns,
                response_msg->timestamp_ns,
                response_msg->request_ready_ns,
                response_msg->server_recv_ns,
                response_msg->server_dispatch_ns,
                response_msg->server_stack_ns,
                response_msg->request_checksum_verify_ns,
                response_msg->response_queue_reserve_ns,
                response_msg->response_alloc_ns,
                response_msg->response_fill_ns,
                response_msg->response_checksum_generate_ns,
                response_msg->response_visibility_ns,
                response_msg->response_placement_ns,
                response_msg->response_ready_ns);
    }
}

static int call_agent(mr_endpoint *agent, mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *body, size_t body_len, byte_buf *response)
{
    mr_hydra_send_observation request_send = {0};
    hydra_msg response_msg = {0};
    uint64_t response_recv_ns = 0;
    uint64_t response_release_done_ns = 0;
    int err = send_hydra_request_observed(
        agent->ctx, kind, task_id, partition_id, partition_count,
        body, body_len, &request_send);
    if (err != 0) {
        return err;
    }
    mr_wire_header hdr = {0};
    err = recv_hydra_response_observed(
        agent->ctx, &hdr, response, &response_msg, &response_recv_ns,
        &response_release_done_ns);
    if (err != 0) {
        return err;
    }
    if (hdr.kind != kind || hdr.task_id != task_id) {
        return EPROTO;
    }
    if (kind == MR_TASK_MAP || kind == MR_TASK_REDUCE ||
        kind == MR_TASK_ACTIVATION_PROBE) {
        mr_task_result_v4 result = {0};
        err = unwrap_task_result(response, &result);
        if ((err != 0 && err != EREMOTEIO) || result.kind != kind ||
            result.task_id != task_id) {
            return err != 0 ? err : EPROTO;
        }
        report_task_result_observed(
            &result,
            response_release_done_ns - request_send.call_begin_ns,
            &request_send, &response_msg, response_recv_ns,
            response_release_done_ns);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int collect_map_locations(const byte_buf *response,
                                 location_vec *locations)
{
    if (response->len < sizeof(control_locations_header)) {
        return EPROTO;
    }
    control_locations_header header = {0};
    memcpy(&header, response->data, sizeof(header));
    size_t need = sizeof(header) +
                  (size_t)header.count * sizeof(IntermediateLocation);
    if (need > response->len) {
        return EPROTO;
    }
    const IntermediateLocation *locs =
        (const IntermediateLocation *)(response->data + sizeof(header));
    for (size_t j = 0; j < (size_t)header.count; j++) {
        location_vec_push(locations, &locs[j]);
    }
    return 0;
}

static int collect_reduce_output(mr_partitions *output, uint32_t reducer_id,
                                 const byte_buf *response)
{
    if (reducer_id >= (uint32_t)output->count) {
        return EPROTO;
    }
    return mr_partitions_parse(output, response->data ? response->data : "",
                               response->len, false, (int)reducer_id);
}

static int run_master(const master_args *args)
{
    bool probe_only = args->activation_probes != 0 ||
                      args->activation_warmups != 0;

    if (args->agents.len == 0 ||
        (!probe_only && (args->input == NULL || args->output == NULL))) {
        fprintf(stderr,
                "master requires --agent/--agents and, except in activation-probe mode, --input and --output\n");
        return 2;
    }
    if (probe_only && args->probe_agent >= args->agents.len) {
        fprintf(stderr, "master: probe-agent is outside the agent list\n");
        return 2;
    }
    if (args->payload_max <= sizeof(mr_wire_header) + 64) {
        fprintf(stderr, "payload-max is too small\n");
        return 2;
    }

    int reducer_count = args->reducers ? (int)args->reducers : (int)args->agents.len;
    size_t max_body = (size_t)(args->payload_max - sizeof(mr_wire_header));
    char *input = NULL;
    task_slice *slices = NULL;
    mr_endpoint *agents = NULL;
    location_vec locations = {0};
    schedule_table schedule = {0};
    bool use_file_schedule = false;
    bool use_role_static = false;
    mr_partitions final_output = {0};
    bool final_init = false;
    uint64_t workflow_begin_ns = 0;
    size_t slice_count = 0;
    int rc = 1;

    agents = mr_xcalloc(args->agents.len, sizeof(agents[0]));
    for (size_t i = 0; i < args->agents.len; i++) {
        agents[i].path = args->agents.items[i];
        int err = open_agent_endpoint(&agents[i], args, i);
        if (err != 0) {
            fprintf(stderr, "master: open agent %zu failed: %s\n",
                    i, hydra_strerror(err));
            goto cleanup;
        }
    }

    if (args->ready_file != NULL) {
        int ready_fd = open(args->ready_file,
                            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (ready_fd < 0) {
            fprintf(stderr, "master: create ready file failed: %s\n",
                    strerror(errno));
            goto cleanup;
        }
        if (close(ready_fd) != 0) {
            fprintf(stderr, "master: close ready file failed: %s\n",
                    strerror(errno));
            goto cleanup;
        }
    }
    if (args->start_file != NULL) {
        uint64_t deadline = hydra_now_ns() + UINT64_C(30000000000);

        while (access(args->start_file, F_OK) != 0) {
            if (errno != ENOENT) {
                fprintf(stderr, "master: check start file failed: %s\n",
                        strerror(errno));
                goto cleanup;
            }
            if (hydra_now_ns() >= deadline) {
                fprintf(stderr, "master: timed out waiting for start file\n");
                goto cleanup;
            }
            usleep(1000);
        }
    }

    if (probe_only) {
        uint64_t total = args->activation_warmups + args->activation_probes;

        for (uint64_t i = 0; i < total; i++) {
            mr_activation_probe_request probe = {
                .target_worker_id =
                    (uint32_t)(i % args->workers_per_agent),
                .expected_role = (uint32_t)args->probe_role,
                .flags = i < args->activation_warmups ?
                    MR_TRACE_F_WARMUP : 0,
                .sequence = i,
                .expected_state_checksum = args->probe_state_checksum,
            };
            byte_buf response = {0};
            uint64_t task_id = UINT64_C(0x8000000000000000) + i;
            int err = call_agent(&agents[args->probe_agent],
                                 MR_TASK_ACTIVATION_PROBE, task_id, 0, 1,
                                 &probe, sizeof(probe), &response);
            if (err == 0 &&
                response.len != sizeof(mr_activation_probe_response)) {
                err = EPROTO;
            }
            if (err == 0) {
                mr_activation_probe_response proof = {0};

                memcpy(&proof, response.data, sizeof(proof));
                if (proof.sequence != probe.sequence ||
                    proof.access_count != 1 || proof.proof == 0 ||
                    (probe.expected_state_checksum != 0 &&
                     proof.state_checksum != probe.expected_state_checksum)) {
                    err = EILSEQ;
                }
            }
            byte_buf_clear(&response);
            if (err != 0) {
                fprintf(stderr,
                        "master: activation probe %" PRIu64 " failed: %s\n",
                        i, strerror(err));
                goto cleanup;
            }
        }
        rc = 0;
        goto cleanup;
    }

    workflow_begin_ns = hydra_now_ns();

    size_t input_len = 0;
    input = read_file(args->input, &input_len);
    if (input == NULL) {
        goto cleanup;
    }
    size_t chunk_size = args->map_chunk ? (size_t)args->map_chunk : max_body / 4;
    if (chunk_size == 0 || chunk_size > max_body - sizeof(control_map_task)) {
        chunk_size = max_body - sizeof(control_map_task);
    }
    slices = make_slices(input, input_len, chunk_size, &slice_count);

    if (args->schedule != NULL && strcmp(args->schedule, "role-static") == 0) {
        if (args->agents.len != 2 || args->workers_per_agent == 0) {
            fprintf(stderr,
                    "master: role-static requires exactly two agents and workers-per-agent\n");
            goto cleanup;
        }
        use_role_static = true;
    } else if (args->schedule != NULL && strcmp(args->schedule, "static") != 0) {
        const char *prefix = "file:";
        size_t prefix_len = strlen(prefix);
        if (strncmp(args->schedule, prefix, prefix_len) != 0 ||
            args->schedule[prefix_len] == '\0') {
            fprintf(stderr,
                    "master: --schedule must be static, role-static or file:PATH\n");
            goto cleanup;
        }
        const char *schedule_path = args->schedule + prefix_len;
        int err = parse_schedule_file(schedule_path, &schedule);
        if (err != 0) {
            fprintf(stderr, "master: parse schedule %s failed: %s\n",
                    schedule_path, strerror(err));
            goto cleanup;
        }
        err = validate_schedule_table(&schedule, args->agents.len, slice_count,
                                      (size_t)reducer_count);
        if (err != 0) {
            goto cleanup;
        }
        use_file_schedule = true;
    }

    if (!args->quiet) {
        printf("master: agents=%zu reducers=%d map_tasks=%zu schedule=%s\n",
               args->agents.len, reducer_count, slice_count,
               use_file_schedule || use_role_static ? args->schedule : "static");
        fflush(stdout);
    }

    if (use_role_static) {
        for (size_t base = 0; base < slice_count;
             base += (size_t)args->workers_per_agent) {
            size_t batch = slice_count - base;
            if (batch > args->workers_per_agent) {
                batch = (size_t)args->workers_per_agent;
            }
            mr_hydra_send_observation *send_obs =
                mr_xcalloc(batch, sizeof(*send_obs));
            for (size_t j = 0; j < batch; j++) {
                size_t i = base + j;
                control_map_task task = {
                    .job_id = MR_JOB_ID,
                    .map_task_id = i,
                    .reducer_count = (uint32_t)reducer_count,
                    .target_worker_id = (uint32_t)j,
                    .data_len = slices[i].len,
                };
                byte_buf body = {0};
                byte_buf_append(&body, &task, sizeof(task));
                byte_buf_append(&body, slices[i].data, slices[i].len);
                int err = send_hydra_request_observed(
                    agents[0].ctx, MR_TASK_MAP, i, 0,
                    (uint64_t)reducer_count, body.data, body.len,
                    &send_obs[j]);
                byte_buf_clear(&body);
                if (err != 0) {
                    free(send_obs);
                    goto cleanup;
                }
            }
            for (size_t j = 0; j < batch; j++) {
                mr_wire_header hdr = {0};
                byte_buf response = {0};
                hydra_msg response_msg = {0};
                uint64_t response_recv_ns = 0;
                uint64_t response_release_done_ns = 0;
                int err = recv_hydra_response_observed(
                    agents[0].ctx, &hdr, &response, &response_msg,
                    &response_recv_ns, &response_release_done_ns);
                if (err == 0 &&
                    (hdr.kind != MR_TASK_MAP || hdr.task_id < base ||
                     hdr.task_id >= base + batch)) {
                    err = EPROTO;
                }
                if (err == 0) {
                    mr_task_result_v4 result = {0};
                    err = unwrap_task_result(&response, &result);
                    if ((err == 0 || err == EREMOTEIO) &&
                        (result.kind != MR_TASK_MAP ||
                         result.task_id != hdr.task_id)) {
                        err = EPROTO;
                    }
                    if (err == 0 || err == EREMOTEIO) {
                        size_t index = (size_t)(hdr.task_id - base);
                        report_task_result_observed(
                            &result,
                            response_release_done_ns -
                                send_obs[index].call_begin_ns,
                            &send_obs[index], &response_msg,
                            response_recv_ns, response_release_done_ns);
                    }
                }
                if (err == 0) {
                    err = collect_map_locations(&response, &locations);
                }
                byte_buf_clear(&response);
                if (err != 0) {
                    free(send_obs);
                    goto cleanup;
                }
            }
            free(send_obs);
        }
    } else for (size_t i = 0; i < slice_count; i++) {
        size_t agent_index = i % args->agents.len;
        uint32_t target_worker_id = MR_TARGET_WORKER_AUTO;
        if (use_file_schedule) {
            schedule_entry entry = {0};
            if (!schedule_lookup(&schedule, MR_TASK_MAP, i, &entry)) {
                fprintf(stderr, "master: schedule missing map task %zu\n", i);
                goto cleanup;
            }
            agent_index = entry.node_id;
            target_worker_id = entry.worker_id;
        }
        control_map_task task = {
            .job_id = MR_JOB_ID,
            .map_task_id = i,
            .reducer_count = (uint32_t)reducer_count,
            .target_worker_id = target_worker_id,
            .data_len = slices[i].len,
        };
        byte_buf body = {0};
        byte_buf_append(&body, &task, sizeof(task));
        byte_buf_append(&body, slices[i].data, slices[i].len);
        byte_buf response = {0};
        int err = call_agent(&agents[agent_index], MR_TASK_MAP, i, 0,
                             (uint64_t)reducer_count, body.data, body.len,
                             &response);
        byte_buf_clear(&body);
        if (err != 0) {
            byte_buf_clear(&response);
            goto cleanup;
        }
        err = collect_map_locations(&response, &locations);
        if (err != 0) {
            byte_buf_clear(&response);
            goto cleanup;
        }
        byte_buf_clear(&response);
    }

    mr_partitions_init(&final_output, reducer_count);
    final_init = true;
    if (use_role_static) {
        for (int base = 0; base < reducer_count;
             base += (int)args->workers_per_agent) {
            int batch = reducer_count - base;
            if ((uint64_t)batch > args->workers_per_agent) {
                batch = (int)args->workers_per_agent;
            }
            mr_hydra_send_observation *send_obs =
                mr_xcalloc((size_t)batch, sizeof(*send_obs));
            for (int j = 0; j < batch; j++) {
                int r = base + j;
                location_vec reducer_locations = {0};
                for (size_t i = 0; i < locations.len; i++) {
                    if (locations.items[i].reducer_id == (uint32_t)r) {
                        location_vec_push(&reducer_locations,
                                          &locations.items[i]);
                    }
                }
                control_reduce_task task = {
                    .job_id = MR_JOB_ID,
                    .reducer_id = (uint32_t)r,
                    .reducer_count = (uint32_t)reducer_count,
                    .target_worker_id = (uint32_t)j,
                    .location_count = reducer_locations.len,
                };
                byte_buf body = {0};
                byte_buf_append(&body, &task, sizeof(task));
                if (reducer_locations.len > 0) {
                    byte_buf_append(
                        &body, reducer_locations.items,
                        reducer_locations.len * sizeof(reducer_locations.items[0]));
                }
                int err = send_hydra_request_observed(
                    agents[1].ctx, MR_TASK_REDUCE,
                    slice_count + (uint64_t)r, (uint64_t)r,
                    (uint64_t)reducer_count, body.data, body.len,
                    &send_obs[j]);
                byte_buf_clear(&body);
                location_vec_destroy(&reducer_locations);
                if (err != 0) {
                    free(send_obs);
                    goto cleanup;
                }
            }
            for (int j = 0; j < batch; j++) {
                mr_wire_header hdr = {0};
                byte_buf response = {0};
                hydra_msg response_msg = {0};
                uint64_t response_recv_ns = 0;
                uint64_t response_release_done_ns = 0;
                int err = recv_hydra_response_observed(
                    agents[1].ctx, &hdr, &response, &response_msg,
                    &response_recv_ns, &response_release_done_ns);
                if (err == 0 &&
                    (hdr.kind != MR_TASK_REDUCE ||
                     hdr.partition_id < (uint64_t)base ||
                     hdr.partition_id >= (uint64_t)(base + batch))) {
                    err = EPROTO;
                }
                if (err == 0) {
                    mr_task_result_v4 result = {0};
                    err = unwrap_task_result(&response, &result);
                    if ((err == 0 || err == EREMOTEIO) &&
                        (result.kind != MR_TASK_REDUCE ||
                         result.task_id != hdr.task_id)) {
                        err = EPROTO;
                    }
                    if (err == 0 || err == EREMOTEIO) {
                        size_t index = (size_t)(hdr.partition_id -
                                                (uint64_t)base);
                        report_task_result_observed(
                            &result,
                            response_release_done_ns -
                                send_obs[index].call_begin_ns,
                            &send_obs[index], &response_msg,
                            response_recv_ns, response_release_done_ns);
                    }
                }
                if (err == 0) {
                    err = collect_reduce_output(
                        &final_output, (uint32_t)hdr.partition_id, &response);
                }
                byte_buf_clear(&response);
                if (err != 0) {
                    free(send_obs);
                    goto cleanup;
                }
            }
            free(send_obs);
        }
    } else for (int r = 0; r < reducer_count; r++) {
        size_t agent_index = (size_t)r % args->agents.len;
        uint32_t target_worker_id = MR_TARGET_WORKER_AUTO;
        if (use_file_schedule) {
            schedule_entry entry = {0};
            if (!schedule_lookup(&schedule, MR_TASK_REDUCE, (uint64_t)r,
                                 &entry)) {
                fprintf(stderr, "master: schedule missing reduce task %d\n", r);
                goto cleanup;
            }
            agent_index = entry.node_id;
            target_worker_id = entry.worker_id;
        }
        location_vec reducer_locations = {0};
        for (size_t i = 0; i < locations.len; i++) {
            if (locations.items[i].reducer_id == (uint32_t)r) {
                location_vec_push(&reducer_locations, &locations.items[i]);
            }
        }
        control_reduce_task task = {
            .job_id = MR_JOB_ID,
            .reducer_id = (uint32_t)r,
            .reducer_count = (uint32_t)reducer_count,
            .target_worker_id = target_worker_id,
            .location_count = reducer_locations.len,
        };
        byte_buf body = {0};
        byte_buf_append(&body, &task, sizeof(task));
        if (reducer_locations.len > 0) {
            byte_buf_append(&body, reducer_locations.items,
                            reducer_locations.len * sizeof(reducer_locations.items[0]));
        }
        byte_buf response = {0};
        int err = call_agent(&agents[agent_index], MR_TASK_REDUCE,
                             slice_count + (uint64_t)r, (uint64_t)r,
                             (uint64_t)reducer_count, body.data, body.len,
                             &response);
        byte_buf_clear(&body);
        location_vec_destroy(&reducer_locations);
        if (err != 0) {
            byte_buf_clear(&response);
            goto cleanup;
        }
        err = collect_reduce_output(&final_output, (uint32_t)r, &response);
        byte_buf_clear(&response);
        if (err != 0) {
            goto cleanup;
        }
    }

    rc = mr_partitions_write_text(&final_output, args->output);

cleanup:
    if (workflow_begin_ns != 0) {
        uint64_t workflow_end_ns = hydra_now_ns();
        fprintf(stderr,
                "master_job_v1 status=%d begin_ns=%" PRIu64
                " end_ns=%" PRIu64 " total_ns=%" PRIu64
                " map_tasks=%zu reducers=%d\n",
                rc, workflow_begin_ns, workflow_end_ns,
                workflow_end_ns - workflow_begin_ns, slice_count,
                reducer_count);
    }
    if (agents != NULL) {
        for (size_t i = 0; i < args->agents.len; i++) {
            if (agents[i].ctx != NULL) {
                byte_buf ignored = {0};
                (void)call_agent(&agents[i], MR_TASK_SHUTDOWN, UINT64_MAX - i,
                                 0, 0, "", 0, &ignored);
                byte_buf_clear(&ignored);
                hydra_close(agents[i].ctx);
            }
        }
    }
    if (final_init) {
        mr_partitions_destroy(&final_output);
    }
    location_vec_destroy(&locations);
    schedule_table_destroy(&schedule);
    free(agents);
    free(slices);
    free(input);
    return rc;
}

static void usage(const char *argv0)
{
    fprintf(stderr,
            "usage:\n"
            "  %s master --input=FILE --output=FILE --agent=PATH [--agent=PATH...]\n"
            "  %s agent --path=PATH --node-id=N [--workers=N]\n"
            "  %s cxlfork-caps\n"
            "  %s template --role=map|reduce --key=N [--base=N] [--bytes=N] [--seed=N]\n"
            "       [--value-scale=N] [--test-corrupt-state] (test only)\n"
            "  %s cxlfork-inspect KEY\n"
            "  %s cxlfork-release KEY GENERATION\n"
            "\n"
            "common: [--backend=file|devdax|ivshmem] [--payload-max=N]\n"
            "        [--queue-capacity=N] [--size=N] [--quiet]\n"
            "master: [--map-chunk=N] [--reducers=N]\n"
            "        [--schedule=static|role-static|file:PATH]\n"
            "        [--workers-per-agent=N]\n"
            "        [--activation-probes=N] [--activation-warmups=N]\n"
            "        [--probe-agent=N] [--probe-role=map|reduce]\n"
            "        [--probe-state-checksum=N]\n"
            "        [--ready-file=PATH] (runner synchronization)\n"
            "        [--start-file=PATH] (runner synchronization)\n"
            "        [--connections=N]\n"
            "        [--queue-policy=shared_pair|per_connection]\n"
            "agent: [--store-dir=DIR] [--agent-count=N] [--global-worker-base=N]\n"
            "       [--ready-file=PATH] (runner synchronization)\n"
            "       [--role=map|reduce|both]\n"
            "       [--launch=persistent|localfork|local-prefork|cxlfork]\n"
            "       [--template-base=N] [--template-bytes=N] [--template-seed=N]\n"
            "       [--template-value-scale=N]\n"
            "       [--template-key=N] [--template-generation=N]\n"
            "       [--template-page-checksum=N] [--cxlfork-policy=mow|moa|hybrid|readonly]\n"
            "       [--validation=fast|full]\n"
            "       [--porter-start=eager|lazy]\n"
            "       [--restore-hold-ms=N] (test only, max 10000)\n"
            "       [--shuffle-backend=file|devdax|ivshmem]\n"
            "       [--shuffle-connections=N]\n"
            "       [--queue-policy=shared_pair|per_connection] [--create]\n",
            argv0, argv0, argv0, argv0, argv0, argv0);
}

static int parse_master_args(master_args *args, int argc, char **argv)
{
    args->backend = MR_BACKEND_FILE;
    args->map_size = MR_DEFAULT_MAP_SIZE;
    args->queue_capacity = MR_DEFAULT_QUEUE_CAPACITY;
    args->payload_max = MR_DEFAULT_PAYLOAD_MAX;
    args->connections = MR_DEFAULT_SHUFFLE_CONNECTIONS;
    args->workers_per_agent = MR_DEFAULT_WORKERS_PER_AGENT;
    args->probe_agent = 1;
    args->probe_role = MR_ROLE_REDUCE;
    args->queue_policy = MR_QUEUE_SHARED_PAIR;

    for (int i = 2; i < argc; i++) {
        const char *value = NULL;
        if ((value = arg_value(argv[i], "--backend=")) != NULL) {
            args->backend = mr_backend_parse(value);
        } else if ((value = arg_value(argv[i], "--input=")) != NULL) {
            args->input = value;
        } else if ((value = arg_value(argv[i], "--output=")) != NULL) {
            args->output = value;
        } else if ((value = arg_value(argv[i], "--schedule=")) != NULL) {
            args->schedule = value;
        } else if ((value = arg_value(argv[i], "--ready-file=")) != NULL) {
            args->ready_file = value;
        } else if ((value = arg_value(argv[i], "--start-file=")) != NULL) {
            args->start_file = value;
        } else if ((value = arg_value(argv[i], "--agent=")) != NULL) {
            str_list_push(&args->agents, value);
        } else if ((value = arg_value(argv[i], "--agents=")) != NULL) {
            str_list_add_csv(&args->agents, value);
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
        } else if ((value = arg_value(argv[i], "--connections=")) != NULL) {
            args->connections = mr_parse_u64(value, "connections");
        } else if ((value = arg_value(argv[i], "--workers-per-agent=")) != NULL) {
            args->workers_per_agent =
                mr_parse_u64(value, "workers-per-agent");
        } else if ((value = arg_value(argv[i], "--activation-probes=")) != NULL) {
            args->activation_probes =
                mr_parse_u64(value, "activation-probes");
        } else if ((value = arg_value(argv[i], "--activation-warmups=")) != NULL) {
            args->activation_warmups =
                mr_parse_u64(value, "activation-warmups");
        } else if ((value = arg_value(argv[i], "--probe-agent=")) != NULL) {
            args->probe_agent = mr_parse_u64(value, "probe-agent");
        } else if ((value = arg_value(argv[i], "--probe-role=")) != NULL) {
            if (mr_role_parse(value, &args->probe_role) != 0 ||
                args->probe_role == MR_ROLE_BOTH) {
                fprintf(stderr, "probe-role must be map or reduce\n");
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--probe-state-checksum=")) != NULL) {
            args->probe_state_checksum =
                mr_parse_u64(value, "probe-state-checksum");
        } else if ((value = arg_value(argv[i], "--queue-policy=")) != NULL) {
            args->queue_policy = mr_queue_policy_parse(value);
        } else if (strcmp(argv[i], "--quiet") == 0) {
            args->quiet = true;
        } else {
            fprintf(stderr, "unknown master arg: %s\n", argv[i]);
            return 2;
        }
    }
    if (args->connections == 0 || args->workers_per_agent == 0 ||
        args->workers_per_agent > UINT32_MAX) {
        return 2;
    }
    return 0;
}

static int parse_agent_args(agent_args *args, int argc, char **argv)
{
    args->backend = MR_BACKEND_FILE;
    args->shuffle_backend = MR_BACKEND_FILE;
    args->map_size = MR_DEFAULT_MAP_SIZE;
    args->shuffle_map_size = MR_DEFAULT_MAP_SIZE;
    args->queue_capacity = MR_DEFAULT_QUEUE_CAPACITY;
    args->payload_max = MR_DEFAULT_PAYLOAD_MAX;
    args->workers = MR_DEFAULT_WORKERS_PER_AGENT;
    args->agent_count = 1;
    args->shuffle_connections = MR_DEFAULT_SHUFFLE_CONNECTIONS;
    args->shuffle_queue_policy = MR_QUEUE_SHARED_PAIR;
    args->role = MR_ROLE_BOTH;
    args->launch_mode = MR_LAUNCH_PERSISTENT;
    args->template_base = MR_TEMPLATE_BASE_DEFAULT;
    args->template_bytes = MR_TEMPLATE_BYTES_DEFAULT;
    args->template_seed = MR_TEMPLATE_SEED_DEFAULT;
    args->template_value_scale = MR_TEMPLATE_VALUE_SCALE_DEFAULT;
    args->cxlfork_policy = MR_CXLFORK_POLICY_MOW;
    args->restore_preopen = true;
    args->store_dir = "/tmp/mr-store";

    for (int i = 2; i < argc; i++) {
        const char *value = NULL;
        if ((value = arg_value(argv[i], "--backend=")) != NULL) {
            args->backend = mr_backend_parse(value);
        } else if ((value = arg_value(argv[i], "--shuffle-backend=")) != NULL) {
            args->shuffle_backend = mr_backend_parse(value);
        } else if ((value = arg_value(argv[i], "--path=")) != NULL) {
            args->path = value;
        } else if ((value = arg_value(argv[i], "--node-id=")) != NULL) {
            args->node_id = mr_parse_u64(value, "node-id");
        } else if ((value = arg_value(argv[i], "--workers=")) != NULL) {
            args->workers = mr_parse_u64(value, "workers");
        } else if ((value = arg_value(argv[i], "--agent-count=")) != NULL) {
            args->agent_count = mr_parse_u64(value, "agent-count");
        } else if ((value = arg_value(argv[i], "--global-worker-base=")) != NULL) {
            args->global_worker_base = mr_parse_u64(value, "global-worker-base");
        } else if ((value = arg_value(argv[i], "--store-dir=")) != NULL) {
            args->store_dir = value;
        } else if ((value = arg_value(argv[i], "--ready-file=")) != NULL) {
            args->ready_file = value;
        } else if ((value = arg_value(argv[i], "--role=")) != NULL) {
            if (mr_role_parse(value, &args->role) != 0) {
                fprintf(stderr, "invalid role: %s\n", value);
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--launch=")) != NULL) {
            if (mr_launch_mode_parse(value, &args->launch_mode) != 0) {
                fprintf(stderr, "invalid launch mode: %s\n", value);
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--template-base=")) != NULL) {
            args->template_base = mr_parse_u64(value, "template-base");
        } else if ((value = arg_value(argv[i], "--template-bytes=")) != NULL) {
            args->template_bytes = mr_parse_u64(value, "template-bytes");
        } else if ((value = arg_value(argv[i], "--template-seed=")) != NULL) {
            args->template_seed = mr_parse_u64(value, "template-seed");
        } else if ((value = arg_value(argv[i], "--template-value-scale=")) != NULL) {
            uint64_t scale = mr_parse_u64(value, "template-value-scale");
            if (scale == 0 || scale > 1024) {
                fprintf(stderr, "template-value-scale must be in [1, 1024]\n");
                return 2;
            }
            args->template_value_scale = (uint32_t)scale;
        } else if ((value = arg_value(argv[i], "--template-key=")) != NULL) {
            args->template_key = mr_parse_u64(value, "template-key");
        } else if ((value = arg_value(argv[i], "--template-generation=")) != NULL) {
            args->template_generation = mr_parse_u64(value, "template-generation");
        } else if ((value = arg_value(argv[i], "--template-page-checksum=")) != NULL) {
            args->template_page_checksum =
                mr_parse_u64(value, "template-page-checksum");
        } else if ((value = arg_value(argv[i], "--cxlfork-policy=")) != NULL) {
            if (mr_cxlfork_policy_parse(value, &args->cxlfork_policy) != 0) {
                fprintf(stderr, "invalid CXLfork policy: %s\n", value);
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--validation=")) != NULL) {
            if (strcmp(value, "fast") == 0) {
                args->validate_full = false;
            } else if (strcmp(value, "full") == 0) {
                args->validate_full = true;
            } else {
                fprintf(stderr, "validation must be fast or full\n");
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--restore-setup=")) != NULL) {
            if (strcmp(value, "preopen") == 0) {
                args->restore_preopen = true;
            } else if (strcmp(value, "per-request") == 0) {
                args->restore_preopen = false;
            } else {
                fprintf(stderr,
                        "restore-setup must be preopen or per-request\n");
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--porter-start=")) != NULL) {
            if (strcmp(value, "eager") == 0) {
                args->lazy_porter = false;
            } else if (strcmp(value, "lazy") == 0) {
                args->lazy_porter = true;
            } else {
                fprintf(stderr, "porter-start must be eager or lazy\n");
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--restore-hold-ms=")) != NULL) {
            args->restore_hold_ms = mr_parse_u64(value, "restore-hold-ms");
            if (args->restore_hold_ms > 10000) {
                fprintf(stderr, "restore-hold-ms must be <= 10000\n");
                return 2;
            }
        } else if ((value = arg_value(argv[i], "--payload-max=")) != NULL) {
            args->payload_max = mr_parse_u64(value, "payload-max");
        } else if ((value = arg_value(argv[i], "--queue-capacity=")) != NULL) {
            args->queue_capacity = mr_parse_u64(value, "queue-capacity");
        } else if ((value = arg_value(argv[i], "--size=")) != NULL) {
            args->map_size = mr_parse_u64(value, "size");
            args->shuffle_map_size = args->map_size;
        } else if ((value = arg_value(argv[i], "--shuffle-size=")) != NULL) {
            args->shuffle_map_size = mr_parse_u64(value, "shuffle-size");
        } else if ((value = arg_value(argv[i], "--shuffle-connections=")) != NULL) {
            args->shuffle_connections = mr_parse_u64(value, "shuffle-connections");
        } else if ((value = arg_value(argv[i], "--queue-policy=")) != NULL) {
            args->shuffle_queue_policy = mr_queue_policy_parse(value);
        } else if (strcmp(argv[i], "--create") == 0) {
            args->create = true;
        } else if (strcmp(argv[i], "--quiet") == 0) {
            args->quiet = true;
        } else {
            fprintf(stderr, "unknown agent arg: %s\n", argv[i]);
            return 2;
        }
    }
    if (args->path == NULL || args->workers == 0 || args->agent_count == 0 ||
        args->shuffle_connections == 0 || args->workers > UINT32_MAX) {
        return 2;
    }
    if (args->launch_mode == MR_LAUNCH_CXLFORK && args->role == MR_ROLE_BOTH) {
        fprintf(stderr, "cxlfork launch requires a single map or reduce role\n");
        return 2;
    }
    if (args->launch_mode == MR_LAUNCH_CXLFORK &&
        (args->template_key == 0 || args->template_generation == 0 ||
         args->template_page_checksum == 0)) {
        fprintf(stderr, "cxlfork launch requires template key, generation and checksum\n");
        return 2;
    }
    if (args->role != MR_ROLE_MAP && args->workers > 1 &&
        args->shuffle_queue_policy == MR_QUEUE_SHARED_PAIR &&
        args->shuffle_connections < args->workers) {
        fprintf(stderr,
                "concurrent reducers require shuffle-connections >= workers\n");
        return 2;
    }
    if (args->global_worker_base == 0) {
        args->global_worker_base = args->node_id * args->workers;
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
            str_list_destroy(&args.agents);
            return err;
        }
        int rc = run_master(&args);
        str_list_destroy(&args.agents);
        return rc;
    }

    if (strcmp(argv[1], "agent") == 0) {
        agent_args args = {0};
        int err = parse_agent_args(&args, argc, argv);
        if (err != 0) {
            usage(argv[0]);
            return err;
        }
        return run_agent(&args, map, reduce,
                         partition ? partition : MR_DefaultHashPartition);
    }

    if (strcmp(argv[1], "cxlfork-caps") == 0) {
        return mr_cxlfork_caps_main(argc, argv);
    }

    if (strcmp(argv[1], "template") == 0) {
        return mr_cxlfork_template_main(argc, argv);
    }

    if (strcmp(argv[1], "cxlfork-inspect") == 0) {
        return mr_cxlfork_inspect_main(argc, argv);
    }

    if (strcmp(argv[1], "cxlfork-release") == 0) {
        return mr_cxlfork_release_main(argc, argv);
    }

    usage(argv[0]);
    return 2;
}
