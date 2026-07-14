#define _GNU_SOURCE

#include "hydra/hydra.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define MR_MAGIC 0x4d524850U
#define MR_VERSION 1U
#define MR_OP 100U
#define MR_DEFAULT_PAYLOAD_MAX (1024U * 1024U)
#define MR_DEFAULT_QUEUE_CAPACITY 4096U
#define MR_DEFAULT_MAP_SIZE (64ULL * 1024ULL * 1024ULL)

typedef enum mr_kind {
    MR_KIND_MAP = 1,
    MR_KIND_REDUCE = 2,
    MR_KIND_SHUTDOWN = 3,
    MR_KIND_ERROR = 4,
} mr_kind;

typedef enum mr_backend {
    MR_BACKEND_FILE,
    MR_BACKEND_DEVDAX,
    MR_BACKEND_IVSHMEM,
} mr_backend;

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

typedef struct str_list {
    char **items;
    size_t len;
    size_t cap;
} str_list;

typedef struct byte_buf {
    char *data;
    size_t len;
    size_t cap;
} byte_buf;

typedef struct wc_entry {
    char *word;
    uint64_t count;
    bool used;
} wc_entry;

typedef struct wc_table {
    wc_entry *entries;
    size_t cap;
    size_t len;
} wc_table;

typedef struct mr_endpoint {
    const char *path;
    hydra_ctx *ctx;
    bool busy;
    uint64_t task_id;
} mr_endpoint;

typedef struct task_slice {
    const char *data;
    size_t len;
} task_slice;

typedef struct reduce_job {
    byte_buf payload;
} reduce_job;

typedef struct master_args {
    mr_backend backend;
    str_list workers;
    const char *input;
    const char *output;
    uint64_t map_size;
    uint64_t queue_capacity;
    uint64_t payload_max;
    uint64_t map_chunk;
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

static void *xcalloc(size_t n, size_t size)
{
    void *ptr = calloc(n, size);
    if (!ptr) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }
    return ptr;
}

static void *xrealloc(void *ptr, size_t size)
{
    void *next = realloc(ptr, size);
    if (!next) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }
    return next;
}

static char *xstrndup(const char *s, size_t len)
{
    char *out = xcalloc(len + 1, 1);
    memcpy(out, s, len);
    return out;
}

static char *xstrdup(const char *s)
{
    return xstrndup(s, strlen(s));
}

static uint64_t parse_u64(const char *value, const char *name)
{
    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 0);
    if (errno != 0 || !end || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }
    return (uint64_t)parsed;
}

static const char *arg_value(const char *arg, const char *prefix)
{
    size_t len = strlen(prefix);
    return strncmp(arg, prefix, len) == 0 ? arg + len : NULL;
}

static mr_backend parse_backend(const char *value)
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

static const char *backend_name(mr_backend backend)
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

static void str_list_push(str_list *list, const char *value)
{
    if (list->len == list->cap) {
        list->cap = list->cap ? list->cap * 2 : 4;
        list->items = xrealloc(list->items, list->cap * sizeof(list->items[0]));
    }
    list->items[list->len++] = xstrdup(value);
}

static void str_list_add_csv(str_list *list, const char *csv)
{
    const char *start = csv;
    for (const char *p = csv;; p++) {
        if (*p == ',' || *p == '\0') {
            if (p > start) {
                char *item = xstrndup(start, (size_t)(p - start));
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

static void byte_buf_reserve(byte_buf *buf, size_t need)
{
    if (need <= buf->cap) {
        return;
    }
    size_t cap = buf->cap ? buf->cap : 1024;
    while (cap < need) {
        cap *= 2;
    }
    buf->data = xrealloc(buf->data, cap);
    buf->cap = cap;
}

static void byte_buf_append(byte_buf *buf, const void *data, size_t len)
{
    byte_buf_reserve(buf, buf->len + len + 1);
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    buf->data[buf->len] = '\0';
}

static void byte_buf_appendf(byte_buf *buf, const char *word, uint64_t count)
{
    char tmp[64];
    int n = snprintf(tmp, sizeof(tmp), "\t%" PRIu64 "\n", count);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        fprintf(stderr, "format error\n");
        exit(1);
    }
    byte_buf_append(buf, word, strlen(word));
    byte_buf_append(buf, tmp, (size_t)n);
}

static uint64_t hash_bytes(const char *data, size_t len)
{
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (unsigned char)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static uint64_t hash_cstr(const char *s)
{
    return hash_bytes(s, strlen(s));
}

static void wc_table_init(wc_table *table)
{
    table->cap = 1024;
    table->entries = xcalloc(table->cap, sizeof(table->entries[0]));
}

static void wc_table_destroy(wc_table *table)
{
    if (!table->entries) {
        return;
    }
    for (size_t i = 0; i < table->cap; i++) {
        free(table->entries[i].word);
    }
    free(table->entries);
    memset(table, 0, sizeof(*table));
}

static void wc_table_rehash(wc_table *table)
{
    wc_entry *old = table->entries;
    size_t old_cap = table->cap;
    table->cap *= 2;
    table->entries = xcalloc(table->cap, sizeof(table->entries[0]));
    table->len = 0;

    for (size_t i = 0; i < old_cap; i++) {
        if (!old[i].used) {
            continue;
        }
        uint64_t hash = hash_cstr(old[i].word);
        for (size_t probe = 0;; probe++) {
            size_t idx = (size_t)((hash + probe) & (table->cap - 1));
            if (!table->entries[idx].used) {
                table->entries[idx] = old[i];
                table->len++;
                break;
            }
        }
    }
    free(old);
}

static void wc_table_add(wc_table *table, const char *word, size_t len, uint64_t count)
{
    if (len == 0 || count == 0) {
        return;
    }
    if (!table->entries) {
        wc_table_init(table);
    }
    if ((table->len + 1) * 10 >= table->cap * 7) {
        wc_table_rehash(table);
    }

    uint64_t hash = hash_bytes(word, len);
    for (size_t probe = 0;; probe++) {
        size_t idx = (size_t)((hash + probe) & (table->cap - 1));
        wc_entry *entry = &table->entries[idx];
        if (!entry->used) {
            entry->word = xstrndup(word, len);
            entry->count = count;
            entry->used = true;
            table->len++;
            return;
        }
        if (strlen(entry->word) == len && memcmp(entry->word, word, len) == 0) {
            entry->count += count;
            return;
        }
    }
}

static int entry_cmp(const void *a, const void *b)
{
    const wc_entry *ea = *(const wc_entry *const *)a;
    const wc_entry *eb = *(const wc_entry *const *)b;
    return strcmp(ea->word, eb->word);
}

static wc_entry **wc_sorted_entries(const wc_table *table, size_t *out_len)
{
    wc_entry **items = xcalloc(table->len ? table->len : 1, sizeof(items[0]));
    size_t len = 0;
    for (size_t i = 0; i < table->cap; i++) {
        if (table->entries[i].used) {
            items[len++] = &table->entries[i];
        }
    }
    qsort(items, len, sizeof(items[0]), entry_cmp);
    *out_len = len;
    return items;
}

static void parse_words_into(wc_table *table, const char *data, size_t len)
{
    byte_buf token = {0};
    for (size_t i = 0; i <= len; i++) {
        unsigned char ch = i < len ? (unsigned char)data[i] : 0;
        if (i < len && (isalnum(ch) || ch == '_')) {
            char lower = (char)tolower(ch);
            byte_buf_append(&token, &lower, 1);
        } else if (token.len > 0) {
            wc_table_add(table, token.data, token.len, 1);
            token.len = 0;
            if (token.data) {
                token.data[0] = '\0';
            }
        }
    }
    free(token.data);
}

static int parse_pairs_into(wc_table *table, const char *data, size_t len)
{
    size_t pos = 0;
    while (pos < len) {
        size_t line_start = pos;
        while (pos < len && data[pos] != '\n') {
            pos++;
        }
        size_t line_len = pos - line_start;
        if (pos < len && data[pos] == '\n') {
            pos++;
        }
        if (line_len == 0) {
            continue;
        }

        const char *line = data + line_start;
        size_t sep = 0;
        while (sep < line_len && line[sep] != '\t' && line[sep] != ' ') {
            sep++;
        }
        if (sep == 0 || sep == line_len) {
            return EINVAL;
        }
        size_t value_pos = sep;
        while (value_pos < line_len && (line[value_pos] == '\t' || line[value_pos] == ' ')) {
            value_pos++;
        }
        if (value_pos == line_len) {
            return EINVAL;
        }

        char count_buf[32];
        size_t count_len = line_len - value_pos;
        if (count_len >= sizeof(count_buf)) {
            return EINVAL;
        }
        memcpy(count_buf, line + value_pos, count_len);
        count_buf[count_len] = '\0';
        errno = 0;
        char *end = NULL;
        unsigned long long count = strtoull(count_buf, &end, 10);
        if (errno != 0 || !end || *end != '\0') {
            return EINVAL;
        }
        wc_table_add(table, line, sep, (uint64_t)count);
    }
    return 0;
}

static void serialize_table(const wc_table *table, byte_buf *out)
{
    size_t len = 0;
    wc_entry **items = wc_sorted_entries(table, &len);
    for (size_t i = 0; i < len; i++) {
        byte_buf_appendf(out, items[i]->word, items[i]->count);
    }
    free(items);
}

static byte_buf make_wire_payload(mr_kind kind, uint64_t task_id,
                                  uint64_t partition_id, uint64_t partition_count,
                                  const void *payload, size_t payload_len)
{
    byte_buf out = {0};
    mr_wire_header hdr = {
        .magic = MR_MAGIC,
        .version = MR_VERSION,
        .kind = (uint32_t)kind,
        .reserved = 0,
        .task_id = task_id,
        .partition_id = partition_id,
        .partition_count = partition_count,
        .payload_len = payload_len,
    };
    byte_buf_append(&out, &hdr, sizeof(hdr));
    if (payload_len > 0) {
        byte_buf_append(&out, payload, payload_len);
    }
    return out;
}

static int parse_wire_payload(const uint8_t *payload, uint32_t len,
                              mr_wire_header *hdr, const char **body,
                              size_t *body_len)
{
    if (len < sizeof(*hdr)) {
        return EINVAL;
    }
    memcpy(hdr, payload, sizeof(*hdr));
    if (hdr->magic != MR_MAGIC || hdr->version != MR_VERSION ||
        hdr->payload_len > len - sizeof(*hdr)) {
        return EINVAL;
    }
    *body = (const char *)payload + sizeof(*hdr);
    *body_len = (size_t)hdr->payload_len;
    return 0;
}

static int open_hydra(hydra_ctx **ctx, mr_backend backend, const char *path,
                      uint64_t map_size, uint64_t queue_capacity,
                      uint64_t payload_max, bool create)
{
    hydra_open_options opts = {
        .path = path,
        .map_size = map_size,
        .queue_capacity = queue_capacity,
        .payload_max = payload_max,
        .connections = 1,
        .connection_id = 0,
        .node_id = 0,
        .peer_node_id = 1,
        .service_id = 1,
        .queue_policy = HYDRA_QUEUE_POLICY_PER_CONNECTION,
        .create = create,
        .truncate_file = create && backend == MR_BACKEND_FILE,
        .checksum_enabled = true,
    };

    switch (backend) {
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

static int send_request_poll(hydra_ctx *ctx, uint64_t req_id, const byte_buf *payload)
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

static int send_response_poll(hydra_ctx *ctx, const hydra_msg *request,
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

static int recv_request_poll(hydra_ctx *ctx, uint64_t *offset,
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

static int try_recv_response(hydra_ctx *ctx, uint64_t *offset,
                             const hydra_msg **msg, const uint8_t **payload)
{
    return hydra_recv_response(ctx, offset, msg, payload);
}

static byte_buf worker_map(const char *data, size_t len)
{
    wc_table table = {0};
    byte_buf out = {0};
    wc_table_init(&table);
    parse_words_into(&table, data, len);
    serialize_table(&table, &out);
    wc_table_destroy(&table);
    return out;
}

static int worker_reduce(const char *data, size_t len, byte_buf *out)
{
    wc_table table = {0};
    wc_table_init(&table);
    int err = parse_pairs_into(&table, data, len);
    if (err == 0) {
        serialize_table(&table, out);
    }
    wc_table_destroy(&table);
    return err;
}

static int run_worker(const worker_args *args)
{
    hydra_ctx *ctx = NULL;
    int err = open_hydra(&ctx, args->backend, args->path, args->map_size,
                         args->queue_capacity, args->payload_max, args->create);
    if (err != 0) {
        fprintf(stderr, "worker: open %s backend %s failed: %s\n",
                backend_name(args->backend), args->path, hydra_strerror(err));
        return 1;
    }

    if (!args->quiet) {
        printf("worker[%" PRIu64 "]: backend=%s path=%s payload_max=%" PRIu64
               " queue=%" PRIu64 " notify=hydrarpc-poll\n",
               args->worker_id, backend_name(args->backend), args->path,
               hydra_payload_max(ctx), hydra_queue_capacity(ctx));
        fflush(stdout);
    }

    for (;;) {
        uint64_t offset = 0;
        const hydra_msg *msg = NULL;
        const uint8_t *payload = NULL;
        err = recv_request_poll(ctx, &offset, &msg, &payload);
        if (err != 0) {
            fprintf(stderr, "worker: recv request failed: %s\n", hydra_strerror(err));
            hydra_close(ctx);
            return 1;
        }

        mr_wire_header hdr = {0};
        const char *body = NULL;
        size_t body_len = 0;
        err = parse_wire_payload(payload, msg->len, &hdr, &body, &body_len);
        byte_buf response_body = {0};
        mr_kind response_kind = (mr_kind)hdr.kind;
        uint32_t status = 0;

        if (err != 0) {
            const char *text = "invalid wire payload\n";
            byte_buf_append(&response_body, text, strlen(text));
            response_kind = MR_KIND_ERROR;
            status = 1;
        } else if (hdr.kind == MR_KIND_SHUTDOWN) {
            byte_buf wire = make_wire_payload(MR_KIND_SHUTDOWN, hdr.task_id, 0, 0, "", 0);
            err = send_response_poll(ctx, msg, 0, &wire);
            free(wire.data);
            hydra_release_message(ctx, offset);
            hydra_close(ctx);
            return err == 0 ? 0 : 1;
        } else if (hdr.kind == MR_KIND_MAP) {
            response_body = worker_map(body, body_len);
        } else if (hdr.kind == MR_KIND_REDUCE) {
            err = worker_reduce(body, body_len, &response_body);
            if (err != 0) {
                const char *text = "reduce input parse failed\n";
                free(response_body.data);
                memset(&response_body, 0, sizeof(response_body));
                byte_buf_append(&response_body, text, strlen(text));
                response_kind = MR_KIND_ERROR;
                status = 1;
            }
        } else {
            const char *text = "unknown task kind\n";
            byte_buf_append(&response_body, text, strlen(text));
            response_kind = MR_KIND_ERROR;
            status = 1;
        }

        if (response_body.len + sizeof(mr_wire_header) > args->payload_max) {
            free(response_body.data);
            memset(&response_body, 0, sizeof(response_body));
            const char *text = "response exceeds payload-max; increase --payload-max or lower --map-chunk\n";
            byte_buf_append(&response_body, text, strlen(text));
            response_kind = MR_KIND_ERROR;
            status = 1;
        }

        byte_buf wire = make_wire_payload(response_kind, hdr.task_id, hdr.partition_id,
                                          hdr.partition_count, response_body.data,
                                          response_body.len);
        err = send_response_poll(ctx, msg, status, &wire);
        free(response_body.data);
        free(wire.data);
        int release_err = hydra_release_message(ctx, offset);
        if (err != 0 || release_err != 0) {
            fprintf(stderr, "worker: send/release failed: %s\n",
                    err ? hydra_strerror(err) : hydra_strerror(release_err));
            hydra_close(ctx);
            return 1;
        }
    }
}

static char *read_file(const char *path, size_t *out_len)
{
    FILE *fp = fopen(path, "rb");
    if (!fp) {
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
    char *data = xcalloc((size_t)size + 1, 1);
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
            slices = xrealloc(slices, cap * sizeof(slices[0]));
        }
        slices[count++] = (task_slice){.data = data + pos, .len = end - pos};
        pos = end;
    }
    if (count == 0) {
        slices = xcalloc(1, sizeof(slices[0]));
    }
    *out_count = count;
    return slices;
}

static int send_task(mr_endpoint *ep, mr_kind kind, uint64_t task_id,
                     uint64_t partition_id, uint64_t partition_count,
                     const void *data, size_t len)
{
    byte_buf wire = make_wire_payload(kind, task_id, partition_id, partition_count, data, len);
    int err = send_request_poll(ep->ctx, task_id, &wire);
    free(wire.data);
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
            int err = try_recv_response(eps[i].ctx, &offset, &msg, &payload);
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
            err = parse_wire_payload(payload, msg->len, hdr, &wire_body, &wire_body_len);
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
            if (err != 0 || status != 0 || hdr->kind == MR_KIND_ERROR) {
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

static reduce_job *make_reduce_jobs(const wc_table *table, size_t max_body,
                                    size_t *out_count)
{
    size_t entry_count = 0;
    wc_entry **entries = wc_sorted_entries(table, &entry_count);
    reduce_job *jobs = NULL;
    size_t count = 0;
    size_t cap = 0;

    for (size_t i = 0; i < entry_count; i++) {
        char tmp[64];
        int n = snprintf(tmp, sizeof(tmp), "\t%" PRIu64 "\n", entries[i]->count);
        if (n < 0 || (size_t)n >= sizeof(tmp)) {
            exit(1);
        }
        size_t line_len = strlen(entries[i]->word) + (size_t)n;
        if (line_len > max_body) {
            fprintf(stderr, "word entry exceeds payload-max: %s\n", entries[i]->word);
            free(entries);
            return NULL;
        }
        if (count == 0 || jobs[count - 1].payload.len + line_len > max_body) {
            if (count == cap) {
                cap = cap ? cap * 2 : 16;
                jobs = xrealloc(jobs, cap * sizeof(jobs[0]));
            }
            memset(&jobs[count], 0, sizeof(jobs[count]));
            count++;
        }
        byte_buf_appendf(&jobs[count - 1].payload, entries[i]->word, entries[i]->count);
    }
    free(entries);
    *out_count = count;
    return jobs;
}

static int write_output(const char *path, byte_buf *results, size_t count)
{
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        fprintf(stderr, "open output %s failed: %s\n", path, strerror(errno));
        return 1;
    }
    for (size_t i = 0; i < count; i++) {
        if (results[i].len > 0 &&
            fwrite(results[i].data, 1, results[i].len, fp) != results[i].len) {
            fprintf(stderr, "write output failed\n");
            fclose(fp);
            return 1;
        }
    }
    fclose(fp);
    return 0;
}

static int shutdown_workers(mr_endpoint *eps, size_t worker_count)
{
    for (size_t i = 0; i < worker_count; i++) {
        if (eps[i].ctx && !eps[i].busy) {
            int err = send_task(&eps[i], MR_KIND_SHUTDOWN, UINT64_MAX - i, 0, 0, "", 0);
            if (err != 0) {
                fprintf(stderr, "master: shutdown send to worker %zu failed: %s\n",
                        i, hydra_strerror(err));
            }
        }
    }
    for (size_t i = 0; i < worker_count; i++) {
        if (!eps[i].ctx || !eps[i].busy) {
            continue;
        }
        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        (void)collect_one_response(eps, worker_count, &worker_index, &hdr, &body);
        free(body.data);
    }
    return 0;
}

static int run_master(const master_args *args)
{
    if (args->workers.len == 0 || !args->input || !args->output) {
        fprintf(stderr, "master requires --input, --output and at least one --worker/--workers\n");
        return 2;
    }
    if (args->payload_max <= sizeof(mr_wire_header) + 64) {
        fprintf(stderr, "payload-max is too small\n");
        return 2;
    }

    mr_endpoint *eps = xcalloc(args->workers.len, sizeof(eps[0]));
    for (size_t i = 0; i < args->workers.len; i++) {
        eps[i].path = args->workers.items[i];
        int err = open_hydra(&eps[i].ctx, args->backend, eps[i].path, args->map_size,
                             args->queue_capacity, args->payload_max, true);
        if (err != 0) {
            fprintf(stderr, "master: open worker %zu path %s failed: %s\n",
                    i, eps[i].path, hydra_strerror(err));
            return 1;
        }
    }

    size_t input_len = 0;
    char *input = read_file(args->input, &input_len);
    if (!input) {
        return 1;
    }

    size_t max_body = (size_t)(args->payload_max - sizeof(mr_wire_header));
    size_t chunk_size = args->map_chunk ? (size_t)args->map_chunk : max_body / 4;
    if (chunk_size == 0 || chunk_size > max_body) {
        chunk_size = max_body;
    }

    size_t slice_count = 0;
    task_slice *slices = make_slices(input, input_len, chunk_size, &slice_count);
    wc_table combined = {0};
    wc_table_init(&combined);

    if (!args->quiet) {
        printf("master: workers=%zu backend=%s payload_max=%" PRIu64
               " map_chunks=%zu notify=hydrarpc-poll\n",
               args->workers.len, backend_name(args->backend), args->payload_max,
               slice_count);
        fflush(stdout);
    }

    size_t next_slice = 0;
    size_t completed = 0;
    while (completed < slice_count) {
        for (size_t i = 0; i < args->workers.len && next_slice < slice_count; i++) {
            if (!eps[i].busy) {
                int err = send_task(&eps[i], MR_KIND_MAP, next_slice, 0, slice_count,
                                    slices[next_slice].data, slices[next_slice].len);
                if (err != 0) {
                    fprintf(stderr, "master: send map task failed: %s\n", hydra_strerror(err));
                    return 1;
                }
                next_slice++;
            }
        }

        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        int err = collect_one_response(eps, args->workers.len, &worker_index, &hdr, &body);
        if (err != 0) {
            free(body.data);
            return 1;
        }
        if (hdr.kind != MR_KIND_MAP) {
            fprintf(stderr, "master: unexpected map response kind %u\n", hdr.kind);
            free(body.data);
            return 1;
        }
        err = parse_pairs_into(&combined, body.data ? body.data : "", body.len);
        free(body.data);
        if (err != 0) {
            fprintf(stderr, "master: parse map response failed\n");
            return 1;
        }
        completed++;
        (void)worker_index;
    }

    size_t reduce_count = 0;
    reduce_job *jobs = make_reduce_jobs(&combined, max_body, &reduce_count);
    if (!jobs && combined.len > 0) {
        return 1;
    }
    wc_table_destroy(&combined);

    byte_buf *reduce_results = xcalloc(reduce_count ? reduce_count : 1, sizeof(reduce_results[0]));
    size_t next_reduce = 0;
    completed = 0;
    while (completed < reduce_count) {
        for (size_t i = 0; i < args->workers.len && next_reduce < reduce_count; i++) {
            if (!eps[i].busy) {
                int err = send_task(&eps[i], MR_KIND_REDUCE, next_reduce, next_reduce,
                                    reduce_count, jobs[next_reduce].payload.data,
                                    jobs[next_reduce].payload.len);
                if (err != 0) {
                    fprintf(stderr, "master: send reduce task failed: %s\n", hydra_strerror(err));
                    return 1;
                }
                next_reduce++;
            }
        }

        size_t worker_index = 0;
        mr_wire_header hdr = {0};
        byte_buf body = {0};
        int err = collect_one_response(eps, args->workers.len, &worker_index, &hdr, &body);
        if (err != 0) {
            free(body.data);
            return 1;
        }
        if (hdr.kind != MR_KIND_REDUCE || hdr.task_id >= reduce_count) {
            fprintf(stderr, "master: unexpected reduce response\n");
            free(body.data);
            return 1;
        }
        reduce_results[hdr.task_id] = body;
        completed++;
        (void)worker_index;
    }

    int rc = write_output(args->output, reduce_results, reduce_count);
    if (!args->quiet && rc == 0) {
        printf("master: wrote %s with %zu reduce task(s)\n", args->output, reduce_count);
    }

    shutdown_workers(eps, args->workers.len);

    for (size_t i = 0; i < reduce_count; i++) {
        free(reduce_results[i].data);
        free(jobs[i].payload.data);
    }
    for (size_t i = 0; i < args->workers.len; i++) {
        hydra_close(eps[i].ctx);
    }
    free(reduce_results);
    free(jobs);
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
            "master: [--map-chunk=N]\n"
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
        if ((value = arg_value(argv[i], "--backend="))) {
            args->backend = parse_backend(value);
        } else if ((value = arg_value(argv[i], "--input="))) {
            args->input = value;
        } else if ((value = arg_value(argv[i], "--output="))) {
            args->output = value;
        } else if ((value = arg_value(argv[i], "--worker="))) {
            str_list_push(&args->workers, value);
        } else if ((value = arg_value(argv[i], "--workers="))) {
            str_list_add_csv(&args->workers, value);
        } else if ((value = arg_value(argv[i], "--payload-max="))) {
            args->payload_max = parse_u64(value, "payload-max");
        } else if ((value = arg_value(argv[i], "--queue-capacity="))) {
            args->queue_capacity = parse_u64(value, "queue-capacity");
        } else if ((value = arg_value(argv[i], "--size="))) {
            args->map_size = parse_u64(value, "size");
        } else if ((value = arg_value(argv[i], "--map-chunk="))) {
            args->map_chunk = parse_u64(value, "map-chunk");
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
        if ((value = arg_value(argv[i], "--backend="))) {
            args->backend = parse_backend(value);
        } else if ((value = arg_value(argv[i], "--path="))) {
            args->path = value;
        } else if ((value = arg_value(argv[i], "--worker-id="))) {
            args->worker_id = parse_u64(value, "worker-id");
        } else if ((value = arg_value(argv[i], "--payload-max="))) {
            args->payload_max = parse_u64(value, "payload-max");
        } else if ((value = arg_value(argv[i], "--queue-capacity="))) {
            args->queue_capacity = parse_u64(value, "queue-capacity");
        } else if ((value = arg_value(argv[i], "--size="))) {
            args->map_size = parse_u64(value, "size");
        } else if (strcmp(argv[i], "--create") == 0) {
            args->create = true;
        } else if (strcmp(argv[i], "--quiet") == 0) {
            args->quiet = true;
        } else {
            fprintf(stderr, "unknown worker arg: %s\n", argv[i]);
            return 2;
        }
    }
    if (!args->path) {
        fprintf(stderr, "worker requires --path\n");
        return 2;
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }
    if (strcmp(argv[1], "master") == 0) {
        master_args args = {0};
        int err = parse_master_args(&args, argc, argv);
        if (err != 0) {
            usage(argv[0]);
            return err;
        }
        int rc = run_master(&args);
        for (size_t i = 0; i < args.workers.len; i++) {
            free(args.workers.items[i]);
        }
        free(args.workers.items);
        return rc;
    }
    if (strcmp(argv[1], "worker") == 0) {
        worker_args args = {0};
        int err = parse_worker_args(&args, argc, argv);
        if (err != 0) {
            usage(argv[0]);
            return err;
        }
        return run_worker(&args);
    }
    usage(argv[0]);
    return 2;
}
