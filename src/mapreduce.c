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
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t map_chunk;
    uint64_t reducers;
    uint64_t connections;
    uint64_t workers_per_agent;
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
    uint64_t template_key;
    uint64_t template_generation;
    uint64_t template_page_checksum;
    mr_cxlfork_policy cxlfork_policy;
    uint64_t restore_hold_ms;
    bool validate_full;
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

static byte_buf make_wire(mr_task_kind kind, uint64_t task_id,
                          uint64_t partition_id, uint64_t partition_count,
                          const void *data, size_t len)
{
    return mr_wire_make(kind, task_id, partition_id, partition_count, data, len);
}

static int send_hydra_request(hydra_ctx *ctx, mr_task_kind kind, uint64_t task_id,
                              uint64_t partition_id, uint64_t partition_count,
                              const void *data, size_t len)
{
    byte_buf wire = make_wire(kind, task_id, partition_id, partition_count, data, len);
    int err = mr_hydra_send_request_poll(ctx, task_id, &wire);
    byte_buf_clear(&wire);
    return err;
}

static int recv_hydra_response(hydra_ctx *ctx, mr_wire_header *hdr, byte_buf *body)
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

        const char *wire_body = NULL;
        size_t wire_body_len = 0;
        err = mr_wire_parse(payload, msg->len, hdr, &wire_body, &wire_body_len);
        if (err == 0) {
            byte_buf_append(body, wire_body, wire_body_len);
        }
        uint32_t status = msg->status;
        int release_err = hydra_release_message(ctx, offset);
        if (release_err != 0) {
            return release_err;
        }
        if (err != 0 || status != 0 || hdr->kind == MR_TASK_ERROR) {
            if (body->len > 0) {
                fprintf(stderr, "remote error: %.*s\n", (int)body->len, body->data);
            }
            return err != 0 ? err : EPROTO;
        }
        return 0;
    }
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
                         int reducer_count, byte_buf *out)
{
    mr_partitions partitions = {0};
    mr_partitions_init(&partitions, reducer_count);
    set_emit_context(EMIT_MAP, &partitions, partitioner);
    map(data, len);
    clear_emit_context();
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
                            size_t location_count, byte_buf *out)
{
    char spill_path[MR_PATH_MAX];
    int n = snprintf(spill_path, sizeof(spill_path),
                     "%s/reduce-worker-%u-part-%u.spill",
                     args->store_dir, args->global_worker_id, reducer_id);
    if (n < 0 || (size_t)n >= sizeof(spill_path)) {
        return ENAMETOOLONG;
    }
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
        current_reduce_input = &input;
        current_reduce_partition_number = (int)reducer_id;
        set_emit_context(EMIT_REDUCE, &output, MR_DefaultHashPartition);
        mr_partition *partition = &input.items[0];
        while (partition->cursor < partition->len) {
            char *key = partition->pairs[partition->cursor].key;
            reduce(key, reduce_get_next, (int)reducer_id);
            while (partition->cursor < partition->len &&
                   strcmp(partition->pairs[partition->cursor].key, key) == 0) {
                partition->cursor++;
            }
        }
        clear_emit_context();
        current_reduce_input = NULL;
        current_reduce_partition_number = 0;
        err = mr_partitions_serialize(&output, out, 0);
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
                               uint64_t partition_count, const void *payload,
                               size_t payload_len, byte_buf *out)
{
    (void)task_id;
    (void)partition_id;
    (void)partition_count;
    worker_child_args args = *(const worker_child_args *)context;
    args.worker_id = worker_id;
    args.global_worker_id = args.global_worker_base + worker_id;
    (void)mkdir_p(args.store_dir);

    if (kind == MR_TASK_MAP) {
        if (payload_len < sizeof(control_map_task)) {
            return EINVAL;
        }
        control_map_task task = {0};
        memcpy(&task, payload, sizeof(task));
        const char *data = (const char *)payload + sizeof(task);
        if (sizeof(task) + task.data_len > payload_len) {
            return EINVAL;
        }
        return worker_do_map(&args, args.map, args.partitioner, data,
                             (size_t)task.data_len, task.job_id,
                             task.map_task_id, (int)task.reducer_count, out);
    }
    if (kind == MR_TASK_REDUCE) {
        if (payload_len < sizeof(control_reduce_task)) {
            return EINVAL;
        }
        control_reduce_task task = {0};
        memcpy(&task, payload, sizeof(task));
        size_t loc_bytes =
            (size_t)task.location_count * sizeof(IntermediateLocation);
        if (sizeof(task) + loc_bytes > payload_len) {
            return EINVAL;
        }
        const IntermediateLocation *locs =
            (const IntermediateLocation *)((const char *)payload + sizeof(task));
        return worker_do_reduce(&args, args.reduce, task.reducer_id,
                                task.reducer_count, locs,
                                (size_t)task.location_count, out);
    }
    return EINVAL;
}

static int agent_submit_request(mr_porter *porter, size_t worker_count,
                                mr_worker_role role, uint64_t agent_count,
                                const mr_wire_header *hdr, const char *body,
                                size_t body_len, uint32_t *worker_out)
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
    } else {
        return EINVAL;
    }
    int err = mr_porter_submit(porter, worker_index, hdr->kind, hdr->task_id,
                               hdr->partition_id, hdr->partition_count, body,
                               body_len);
    if (err == 0 && worker_out != NULL) {
        *worker_out = worker_index;
    }
    return err;
}

static int run_agent(const agent_args *args, Mapper map, Reducer reduce,
                     Partitioner partitioner)
{
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
        },
        .restore = {
            .template = {
                .role = args->role,
                .key = args->template_key,
                .base = args->template_base,
                .bytes = args->template_bytes,
                .seed = args->template_seed,
            },
            .generation = args->template_generation,
            .page_checksum = args->template_page_checksum,
            .policy = args->cxlfork_policy,
            .validate_full = args->validate_full,
        },
        .restore_hold_ms = (uint32_t)args->restore_hold_ms,
        .task_fn = porter_execute_task,
        .task_context = &worker_context,
    };
    mr_porter *porter = NULL;
    err = mr_porter_start(&porter_config, &porter);
    if (err != 0) {
        fprintf(stderr, "agent: start porter failed: %s\n", strerror(err));
        return 1;
    }

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
        mr_porter_stop(porter);
        return 1;
    }

    if (!args->quiet) {
        printf("agent[%" PRIu64 "]: role=%s launch=%s workers=%" PRIu64
               " endpoint=%s store=%s\n",
               args->node_id, mr_role_name(args->role),
               mr_launch_mode_name(args->launch_mode), args->workers,
               args->path, args->store_dir);
        fflush(stdout);
    }

    typedef struct agent_pending {
        bool active;
        hydra_msg request;
        mr_wire_header header;
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
            hydra_msg request = *msg;
            mr_wire_header hdr = {0};
            const char *body = NULL;
            size_t body_len = 0;
            byte_buf response = {0};
            uint32_t status = 0;
            int request_err =
                mr_wire_parse(payload, msg->len, &hdr, &body, &body_len);
            bool deferred = false;

            if (request_err == 0 &&
                (hdr.kind == MR_TASK_MAP || hdr.kind == MR_TASK_REDUCE)) {
                uint32_t worker = UINT32_MAX;
                request_err = agent_submit_request(
                    porter, (size_t)args->workers, args->role,
                    args->agent_count, &hdr, body, body_len, &worker);
                if (request_err == 0) {
                    if (pending[worker].active) {
                        request_err = EBUSY;
                    } else {
                        pending[worker].active = true;
                        pending[worker].request = request;
                        pending[worker].header = hdr;
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
                (void)mr_hydra_send_response_poll(ctx, &request, status,
                                                  &wire);
                byte_buf_clear(&wire);
                byte_buf_clear(&response);
            }
        } else if (err != EAGAIN) {
            fprintf(stderr, "agent: receive request failed: %s\n",
                    hydra_strerror(err));
            break;
        }

        for (;;) {
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
                       " local_copy_pages=%" PRIu64 "\n",
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
                       porter_result.restore.local_copy_pages);
                fflush(stdout);
            }
            uint32_t status = collect_err == 0 ? 0 : 1;
            if (collect_err != 0) {
                byte_buf_clear(&response);
                const char *text = "agent task failed\n";
                byte_buf_append(&response, text, strlen(text));
            }
            mr_task_result_v1 task_result = {
                .magic = MR_TASK_RESULT_MAGIC,
                .size = sizeof(task_result),
                .version = MR_TASK_RESULT_VERSION,
                .kind = completed.header.kind,
                .status = (uint32_t)(collect_err == 0 ? 0 : collect_err),
                .node_id = (uint32_t)args->node_id,
                .worker_id = porter_result.worker_id,
                .task_id = completed.header.task_id,
                .worker_pid = porter_result.worker_pid,
                .dispatch_ns = porter_result.dispatch_ns,
                .fork_ns = porter_result.fork_ns,
                .wakeup_ns = porter_result.wakeup_ns,
                .restore_ioctl_ns = porter_result.restore_ioctl_ns,
                .template_validate_ns =
                    porter_result.template_validate_ns,
                .task_ns = porter_result.task_ns,
                .reap_ns = porter_result.reap_ns,
                .refill_ns = porter_result.refill_ns,
                .porter_total_ns = porter_result.total_ns,
                .restore_id = porter_result.restore.restore_id,
                .restore_path_mask = porter_result.restore.path_mask,
                .kernel_total_ns = porter_result.restore.kernel_total_ns,
                .shared_leaf_pages =
                    porter_result.restore.shared_leaf_pages,
                .fallback_pages = porter_result.restore.fallback_pages,
                .local_copy_pages = porter_result.restore.local_copy_pages,
                .leaf_copy_pages = porter_result.restore.leaf_copy_pages,
            };
            byte_buf measured = {0};
            byte_buf_append(&measured, &task_result, sizeof(task_result));
            byte_buf_append(&measured, response.data, response.len);
            byte_buf_clear(&response);
            response = measured;
            byte_buf wire = make_wire(
                collect_err == 0 ? (mr_task_kind)completed.header.kind
                                 : MR_TASK_ERROR,
                completed.header.task_id, completed.header.partition_id,
                completed.header.partition_count, response.data,
                response.len);
            (void)mr_hydra_send_response_poll(ctx, &completed.request, status,
                                              &wire);
            byte_buf_clear(&wire);
            byte_buf_clear(&response);
        }
        if (!progress) {
            hydra_cpu_relax();
        }
    }

    free(pending);
    hydra_close(ctx);
    mr_porter_stop(porter);
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

static int unwrap_task_result(byte_buf *response, mr_task_result_v1 *result)
{
    if (response->len < sizeof(*result)) {
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

static void report_task_result(const mr_task_result_v1 *result,
                               uint64_t master_roundtrip_ns)
{
    fprintf(stderr,
            "task_result_v1 task=%" PRIu64 " kind=%u node=%u worker=%u"
            " pid=%" PRId64 " status=%u roundtrip_ns=%" PRIu64
            " porter_total_ns=%" PRIu64 " dispatch_ns=%" PRIu64
            " fork_ns=%" PRIu64 " wakeup_ns=%" PRIu64
            " restore_ioctl_ns=%" PRIu64
            " template_validate_ns=%" PRIu64 " task_ns=%" PRIu64
            " reap_ns=%" PRIu64 " refill_ns=%" PRIu64
            " restore_id=%" PRIu64 " path_mask=%" PRIu64
            " kernel_total_ns=%" PRIu64 " shared_leaf_pages=%" PRIu64
            " fallback_pages=%" PRIu64 " local_copy_pages=%" PRIu64
            " leaf_copy_pages=%" PRIu64 "\n",
            result->task_id, result->kind, result->node_id,
            result->worker_id, result->worker_pid, result->status,
            master_roundtrip_ns, result->porter_total_ns,
            result->dispatch_ns, result->fork_ns, result->wakeup_ns,
            result->restore_ioctl_ns, result->template_validate_ns,
            result->task_ns, result->reap_ns, result->refill_ns,
            result->restore_id, result->restore_path_mask,
            result->kernel_total_ns, result->shared_leaf_pages,
            result->fallback_pages, result->local_copy_pages,
            result->leaf_copy_pages);
}

static int call_agent(mr_endpoint *agent, mr_task_kind kind, uint64_t task_id,
                      uint64_t partition_id, uint64_t partition_count,
                      const void *body, size_t body_len, byte_buf *response)
{
    uint64_t begin_ns = hydra_now_ns();
    int err = send_hydra_request(agent->ctx, kind, task_id, partition_id,
                                 partition_count, body, body_len);
    if (err != 0) {
        return err;
    }
    mr_wire_header hdr = {0};
    err = recv_hydra_response(agent->ctx, &hdr, response);
    if (err != 0) {
        return err;
    }
    if (hdr.kind != kind || hdr.task_id != task_id) {
        return EPROTO;
    }
    if (kind == MR_TASK_MAP || kind == MR_TASK_REDUCE) {
        mr_task_result_v1 result = {0};
        err = unwrap_task_result(response, &result);
        if (err != 0 || result.kind != kind || result.task_id != task_id) {
            return err != 0 ? err : EPROTO;
        }
        report_task_result(&result, hydra_now_ns() - begin_ns);
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
    if (args->agents.len == 0 || args->input == NULL || args->output == NULL) {
        fprintf(stderr, "master requires --input, --output and --agent/--agents\n");
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

    size_t input_len = 0;
    input = read_file(args->input, &input_len);
    if (input == NULL) {
        goto cleanup;
    }
    size_t chunk_size = args->map_chunk ? (size_t)args->map_chunk : max_body / 4;
    if (chunk_size == 0 || chunk_size > max_body - sizeof(control_map_task)) {
        chunk_size = max_body - sizeof(control_map_task);
    }
    size_t slice_count = 0;
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
            uint64_t *sent_at = mr_xcalloc(batch, sizeof(*sent_at));
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
                sent_at[j] = hydra_now_ns();
                int err = send_hydra_request(
                    agents[0].ctx, MR_TASK_MAP, i, 0,
                    (uint64_t)reducer_count, body.data, body.len);
                byte_buf_clear(&body);
                if (err != 0) {
                    free(sent_at);
                    goto cleanup;
                }
            }
            for (size_t j = 0; j < batch; j++) {
                mr_wire_header hdr = {0};
                byte_buf response = {0};
                int err = recv_hydra_response(agents[0].ctx, &hdr, &response);
                if (err == 0 &&
                    (hdr.kind != MR_TASK_MAP || hdr.task_id < base ||
                     hdr.task_id >= base + batch)) {
                    err = EPROTO;
                }
                if (err == 0) {
                    mr_task_result_v1 result = {0};
                    err = unwrap_task_result(&response, &result);
                    if (err == 0 &&
                        (result.kind != MR_TASK_MAP ||
                         result.task_id != hdr.task_id)) {
                        err = EPROTO;
                    }
                    if (err == 0) {
                        report_task_result(
                            &result,
                            hydra_now_ns() - sent_at[hdr.task_id - base]);
                    }
                }
                if (err == 0) {
                    err = collect_map_locations(&response, &locations);
                }
                byte_buf_clear(&response);
                if (err != 0) {
                    free(sent_at);
                    goto cleanup;
                }
            }
            free(sent_at);
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
            uint64_t *sent_at =
                mr_xcalloc((size_t)batch, sizeof(*sent_at));
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
                sent_at[j] = hydra_now_ns();
                int err = send_hydra_request(
                    agents[1].ctx, MR_TASK_REDUCE,
                    slice_count + (uint64_t)r, (uint64_t)r,
                    (uint64_t)reducer_count, body.data, body.len);
                byte_buf_clear(&body);
                location_vec_destroy(&reducer_locations);
                if (err != 0) {
                    free(sent_at);
                    goto cleanup;
                }
            }
            for (int j = 0; j < batch; j++) {
                mr_wire_header hdr = {0};
                byte_buf response = {0};
                int err = recv_hydra_response(agents[1].ctx, &hdr, &response);
                if (err == 0 &&
                    (hdr.kind != MR_TASK_REDUCE ||
                     hdr.partition_id < (uint64_t)base ||
                     hdr.partition_id >= (uint64_t)(base + batch))) {
                    err = EPROTO;
                }
                if (err == 0) {
                    mr_task_result_v1 result = {0};
                    err = unwrap_task_result(&response, &result);
                    if (err == 0 &&
                        (result.kind != MR_TASK_REDUCE ||
                         result.task_id != hdr.task_id)) {
                        err = EPROTO;
                    }
                    if (err == 0) {
                        report_task_result(
                            &result,
                            hydra_now_ns() -
                                sent_at[hdr.partition_id - (uint64_t)base]);
                    }
                }
                if (err == 0) {
                    err = collect_reduce_output(
                        &final_output, (uint32_t)hdr.partition_id, &response);
                }
                byte_buf_clear(&response);
                if (err != 0) {
                    free(sent_at);
                    goto cleanup;
                }
            }
            free(sent_at);
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
            "  %s cxlfork-inspect KEY\n"
            "  %s cxlfork-release KEY GENERATION\n"
            "\n"
            "common: [--backend=file|devdax|ivshmem] [--payload-max=N]\n"
            "        [--queue-capacity=N] [--size=N] [--quiet]\n"
            "master: [--map-chunk=N] [--reducers=N]\n"
            "        [--schedule=static|role-static|file:PATH]\n"
            "        [--workers-per-agent=N]\n"
            "        [--connections=N]\n"
            "        [--queue-policy=shared_pair|per_connection]\n"
            "agent: [--store-dir=DIR] [--agent-count=N] [--global-worker-base=N]\n"
            "       [--role=map|reduce|both]\n"
            "       [--launch=persistent|localfork|local-prefork|cxlfork]\n"
            "       [--template-base=N] [--template-bytes=N] [--template-seed=N]\n"
            "       [--template-key=N] [--template-generation=N]\n"
            "       [--template-page-checksum=N] [--cxlfork-policy=mow|moa|hybrid|readonly]\n"
            "       [--validation=fast|full]\n"
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
    args->cxlfork_policy = MR_CXLFORK_POLICY_MOW;
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
