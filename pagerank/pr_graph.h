#ifndef PR_GRAPH_H
#define PR_GRAPH_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PR_TEMPLATE_MAGIC UINT64_C(0x50524743584c5631)
#define PR_TEMPLATE_VERSION 1U
#define PR_TEMPLATE_BASE_DEFAULT UINT64_C(0x500000000000)
#define PR_TEMPLATE_BYTES_DEFAULT (64ULL * 1024ULL * 1024ULL)
#define PR_TEMPLATE_HEADER_BYTES 4096U

typedef struct pr_edge {
    uint32_t src;
    uint32_t dst;
} pr_edge;

typedef struct pr_edge_set {
    pr_edge *edges;
    uint64_t *offsets;
    uint64_t count;
    uint64_t edge_bytes;
    uint64_t offset_bytes;
} pr_edge_set;

typedef struct pr_graph {
    uint32_t node;
    uint32_t degree;
    uint32_t remote_degree;
    uint32_t reserved;
    uint64_t vertices;
    pr_edge_set local_in;
    pr_edge_set remote_in;
    pr_edge_set remote_out;
    void *owner_mapping;
    uint64_t owner_bytes;
    bool template_backed;
} pr_graph;

typedef struct pr_template_header_v1 {
    uint64_t magic;
    uint32_t version;
    uint32_t header_size;
    uint64_t template_base;
    uint64_t template_bytes;
    uint32_t node;
    uint32_t degree;
    uint32_t remote_degree;
    uint32_t reserved0;
    uint64_t vertices;
    uint64_t local_edges_offset;
    uint64_t local_edges_count;
    uint64_t local_offsets_offset;
    uint64_t remote_in_edges_offset;
    uint64_t remote_in_edges_count;
    uint64_t remote_in_offsets_offset;
    uint64_t remote_out_edges_offset;
    uint64_t remote_out_edges_count;
    uint64_t remote_out_offsets_offset;
    uint64_t graph_bytes;
    uint64_t graph_checksum;
    uint64_t config_fingerprint;
    uint64_t flags;
    uint64_t reserved[42];
} pr_template_header_v1;

uint32_t pr_edge_destination(int src_node, uint32_t src, int edge_index,
                             uint64_t vertices, bool remote);
int pr_graph_build_local(pr_graph *graph, uint32_t node, uint64_t vertices,
                         uint32_t degree, uint32_t remote_degree);
int pr_graph_build_template(void *mapping, uint64_t mapping_bytes,
                            uint32_t node, uint64_t vertices,
                            uint32_t degree, uint32_t remote_degree,
                            pr_graph *graph);
int pr_graph_bind_template(void *mapping, uint64_t mapping_bytes,
                           uint32_t expected_node, uint64_t vertices,
                           uint32_t degree, uint32_t remote_degree,
                           bool full_validate, pr_graph *graph);
void pr_graph_destroy(pr_graph *graph);
uint64_t pr_graph_checksum(const pr_graph *graph);
uint64_t pr_graph_config_fingerprint(uint32_t node, uint64_t vertices,
                                     uint32_t degree,
                                     uint32_t remote_degree);

#endif
