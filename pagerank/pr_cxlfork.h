#ifndef PR_CXLFORK_H
#define PR_CXLFORK_H

#include "pr_graph.h"
#include "mr_cxlfork.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct pr_cxlfork_options {
    uint64_t key;
    uint64_t generation;
    uint64_t page_checksum;
    uint64_t base;
    uint64_t bytes;
    uint64_t vertices;
    uint32_t degree;
    uint32_t remote_degree;
    bool full_validate;
} pr_cxlfork_options;

typedef struct pr_cxlfork_handle {
    mr_cxlfork_session *session;
    mr_cxlfork_worker_context *context;
    mr_cxlfork_result result;
    mr_cxlfork_timing timing;
    uint64_t graph_checksum;
} pr_cxlfork_handle;

int pr_cxlfork_publish(const pr_cxlfork_options *options);
int pr_cxlfork_context_open(const pr_cxlfork_options *options,
                            pr_cxlfork_handle *handle);
int pr_cxlfork_restore(const pr_cxlfork_options *options,
                       pr_cxlfork_handle *handle, pr_graph *graph);
int pr_cxlfork_collect(pr_cxlfork_handle *handle);
void pr_cxlfork_close(pr_cxlfork_handle *handle);
int pr_cxlfork_inspect(uint64_t key, uint64_t expected_generation,
                       bool require_no_readers);
int pr_cxlfork_release(uint64_t key, uint64_t generation);

#endif
