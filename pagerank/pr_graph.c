#define _GNU_SOURCE

#include "pr_graph.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define PR_HUGE_SIZE (2ULL * 1024ULL * 1024ULL)
#define PR_ALIGN 64ULL

_Static_assert(sizeof(pr_edge) == 8, "PageRank edge ABI changed");
_Static_assert(sizeof(pr_template_header_v1) <= PR_TEMPLATE_HEADER_BYTES,
               "PageRank template header exceeds its reserved page");

typedef struct pr_arena {
    unsigned char *base;
    uint64_t bytes;
    uint64_t cursor;
} pr_arena;

static uint64_t align_up(uint64_t value, uint64_t alignment)
{
    return (value + alignment - 1) & ~(alignment - 1);
}

static uint64_t mix64(uint64_t value)
{
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

uint32_t pr_edge_destination(int src_node, uint32_t src, int edge_index,
                             uint64_t vertices, bool remote)
{
    uint64_t key = ((uint64_t)(src_node + 1) << 60) ^
                   ((uint64_t)src << 16) ^ (uint64_t)edge_index;

    key ^= remote ? UINT64_C(0xd1b54a32d192ed03)
                  : UINT64_C(0x9e3779b97f4a7c15);
    return (uint32_t)(mix64(key) % vertices);
}

static int compare_edges(const void *left, const void *right)
{
    const pr_edge *a = left;
    const pr_edge *b = right;

    if (a->dst != b->dst) {
        return a->dst < b->dst ? -1 : 1;
    }
    if (a->src != b->src) {
        return a->src < b->src ? -1 : 1;
    }
    return 0;
}

static int build_edge_set(pr_edge_set *set, uint32_t src_node, bool remote,
                          uint64_t vertices, uint32_t edge_count,
                          pr_edge *edges, uint64_t *offsets)
{
    uint64_t cursor = 0;

    if (vertices > UINT32_MAX || edge_count == 0 ||
        vertices > UINT64_MAX / edge_count) {
        return EOVERFLOW;
    }
    set->count = vertices * edge_count;
    set->edge_bytes = set->count * sizeof(*set->edges);
    set->offset_bytes = (vertices + 1) * sizeof(*set->offsets);
    set->edges = edges;
    set->offsets = offsets;
    memset(offsets, 0, (size_t)set->offset_bytes);

    for (uint32_t src = 0; src < vertices; src++) {
        for (uint32_t edge = 0; edge < edge_count; edge++) {
            set->edges[cursor].src = src;
            set->edges[cursor].dst =
                pr_edge_destination((int)src_node, src, (int)edge,
                                    vertices, remote);
            cursor++;
        }
    }
    qsort(set->edges, (size_t)set->count, sizeof(*set->edges), compare_edges);
    for (uint64_t index = 0; index < set->count; index++) {
        set->offsets[set->edges[index].dst + 1]++;
    }
    for (uint64_t vertex = 1; vertex <= vertices; vertex++) {
        set->offsets[vertex] += set->offsets[vertex - 1];
    }
    return 0;
}

static void *alloc_huge_aligned(uint64_t bytes)
{
    uint64_t rounded = align_up(bytes, PR_HUGE_SIZE);
    uint64_t request = rounded + PR_HUGE_SIZE;
    unsigned char *base = mmap(NULL, (size_t)request,
                               PROT_READ | PROT_WRITE,
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    uintptr_t aligned;
    uint64_t prefix;
    uint64_t suffix;

    if (base == MAP_FAILED) {
        return NULL;
    }
    aligned = align_up((uintptr_t)base, PR_HUGE_SIZE);
    prefix = aligned - (uintptr_t)base;
    suffix = request - prefix - rounded;
    if (prefix != 0) {
        (void)munmap(base, (size_t)prefix);
    }
    if (suffix != 0) {
        (void)munmap((void *)(aligned + rounded), (size_t)suffix);
    }
    (void)madvise((void *)aligned, (size_t)rounded, MADV_HUGEPAGE);
    return (void *)aligned;
}

static int alloc_owned_set(pr_edge_set *set, uint64_t vertices,
                           uint32_t edge_count)
{
    if (vertices > UINT64_MAX / edge_count) {
        return EOVERFLOW;
    }
    uint64_t count = vertices * edge_count;
    uint64_t edge_bytes = count * sizeof(pr_edge);
    uint64_t offset_bytes = (vertices + 1) * sizeof(uint64_t);

    set->count = count;
    set->edge_bytes = edge_bytes;
    set->offset_bytes = offset_bytes;
    set->edges = alloc_huge_aligned(edge_bytes);
    set->offsets = alloc_huge_aligned(offset_bytes);
    if (set->edges == NULL || set->offsets == NULL) {
        return ENOMEM;
    }
    return 0;
}

int pr_graph_build_local(pr_graph *graph, uint32_t node, uint64_t vertices,
                         uint32_t degree, uint32_t remote_degree)
{
    int err;

    if (graph == NULL || node > 1 || vertices < 1 || vertices > UINT32_MAX ||
        remote_degree == 0 || remote_degree >= degree) {
        return EINVAL;
    }
    memset(graph, 0, sizeof(*graph));
    graph->node = node;
    graph->vertices = vertices;
    graph->degree = degree;
    graph->remote_degree = remote_degree;

    err = alloc_owned_set(&graph->local_in, vertices,
                          degree - remote_degree);
    if (err == 0) {
        err = alloc_owned_set(&graph->remote_in, vertices, remote_degree);
    }
    if (err == 0) {
        err = alloc_owned_set(&graph->remote_out, vertices, remote_degree);
    }
    if (err != 0) {
        pr_graph_destroy(graph);
        return err;
    }
    err = build_edge_set(&graph->local_in, node, false, vertices,
                         degree - remote_degree, graph->local_in.edges,
                         graph->local_in.offsets);
    if (err == 0) {
        err = build_edge_set(&graph->remote_out, node, true, vertices,
                             remote_degree, graph->remote_out.edges,
                             graph->remote_out.offsets);
    }
    if (err == 0) {
        err = build_edge_set(&graph->remote_in, 1U - node, true, vertices,
                             remote_degree, graph->remote_in.edges,
                             graph->remote_in.offsets);
    }
    if (err != 0) {
        pr_graph_destroy(graph);
    }
    return err;
}

static void *arena_alloc(pr_arena *arena, uint64_t bytes, uint64_t *offset)
{
    uint64_t start = align_up(arena->cursor, PR_ALIGN);

    if (start > arena->bytes || bytes > arena->bytes - start) {
        return NULL;
    }
    arena->cursor = start + bytes;
    if (offset != NULL) {
        *offset = start;
    }
    return arena->base + start;
}

uint64_t pr_graph_config_fingerprint(uint32_t node, uint64_t vertices,
                                     uint32_t degree,
                                     uint32_t remote_degree)
{
    uint64_t value = UINT64_C(0x7072677261706871) ^ vertices;

    value ^= (uint64_t)node << 60;
    value ^= (uint64_t)degree << 32;
    value ^= remote_degree;
    return mix64(value);
}

static uint64_t checksum_bytes(uint64_t hash, const void *data, uint64_t bytes)
{
    const unsigned char *cursor = data;

    for (uint64_t index = 0; index < bytes; index++) {
        hash ^= cursor[index];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

uint64_t pr_graph_checksum(const pr_graph *graph)
{
    uint64_t hash = UINT64_C(1469598103934665603);

    hash = checksum_bytes(hash, graph->local_in.edges,
                          graph->local_in.edge_bytes);
    hash = checksum_bytes(hash, graph->local_in.offsets,
                          graph->local_in.offset_bytes);
    hash = checksum_bytes(hash, graph->remote_in.edges,
                          graph->remote_in.edge_bytes);
    hash = checksum_bytes(hash, graph->remote_in.offsets,
                          graph->remote_in.offset_bytes);
    hash = checksum_bytes(hash, graph->remote_out.edges,
                          graph->remote_out.edge_bytes);
    hash = checksum_bytes(hash, graph->remote_out.offsets,
                          graph->remote_out.offset_bytes);
    return hash;
}

int pr_graph_build_template(void *mapping, uint64_t mapping_bytes,
                            uint32_t node, uint64_t vertices,
                            uint32_t degree, uint32_t remote_degree,
                            pr_graph *graph)
{
    pr_template_header_v1 *header = mapping;
    pr_arena arena = {
        .base = mapping,
        .bytes = mapping_bytes,
        .cursor = PR_TEMPLATE_HEADER_BYTES,
    };
    pr_edge *edges;
    uint64_t *offsets;
    int err;

    if (mapping == NULL || mapping_bytes < PR_TEMPLATE_HEADER_BYTES ||
        graph == NULL || node > 1 || remote_degree == 0 ||
        remote_degree >= degree || vertices == 0 || vertices > UINT32_MAX) {
        return EINVAL;
    }
    memset(graph, 0, sizeof(*graph));
    memset(mapping, 0, (size_t)mapping_bytes);
    graph->node = node;
    graph->vertices = vertices;
    graph->degree = degree;
    graph->remote_degree = remote_degree;
    graph->owner_mapping = mapping;
    graph->owner_bytes = mapping_bytes;
    graph->template_backed = true;

    uint64_t local_count = vertices * (degree - remote_degree);
    uint64_t remote_count = vertices * remote_degree;
    uint64_t offsets_bytes = (vertices + 1) * sizeof(uint64_t);

    edges = arena_alloc(&arena, local_count * sizeof(*edges),
                        &header->local_edges_offset);
    offsets = arena_alloc(&arena, offsets_bytes,
                          &header->local_offsets_offset);
    if (edges == NULL || offsets == NULL) {
        return ENOSPC;
    }
    err = build_edge_set(&graph->local_in, node, false, vertices,
                         degree - remote_degree, edges, offsets);
    if (err != 0) {
        return err;
    }

    edges = arena_alloc(&arena, remote_count * sizeof(*edges),
                        &header->remote_in_edges_offset);
    offsets = arena_alloc(&arena, offsets_bytes,
                          &header->remote_in_offsets_offset);
    if (edges == NULL || offsets == NULL) {
        return ENOSPC;
    }
    err = build_edge_set(&graph->remote_in, 1U - node, true, vertices,
                         remote_degree, edges, offsets);
    if (err != 0) {
        return err;
    }

    edges = arena_alloc(&arena, remote_count * sizeof(*edges),
                        &header->remote_out_edges_offset);
    offsets = arena_alloc(&arena, offsets_bytes,
                          &header->remote_out_offsets_offset);
    if (edges == NULL || offsets == NULL) {
        return ENOSPC;
    }
    err = build_edge_set(&graph->remote_out, node, true, vertices,
                         remote_degree, edges, offsets);
    if (err != 0) {
        return err;
    }

    header->magic = PR_TEMPLATE_MAGIC;
    header->version = PR_TEMPLATE_VERSION;
    header->header_size = sizeof(*header);
    header->template_base = (uintptr_t)mapping;
    header->template_bytes = mapping_bytes;
    header->node = node;
    header->vertices = vertices;
    header->degree = degree;
    header->remote_degree = remote_degree;
    header->local_edges_count = local_count;
    header->remote_in_edges_count = remote_count;
    header->remote_out_edges_count = remote_count;
    header->graph_bytes = arena.cursor - PR_TEMPLATE_HEADER_BYTES;
    header->config_fingerprint =
        pr_graph_config_fingerprint(node, vertices, degree, remote_degree);
    header->graph_checksum = pr_graph_checksum(graph);
    return 0;
}

static bool range_valid(uint64_t offset, uint64_t bytes, uint64_t total)
{
    return offset >= PR_TEMPLATE_HEADER_BYTES && offset <= total &&
           bytes <= total - offset;
}

int pr_graph_bind_template(void *mapping, uint64_t mapping_bytes,
                           uint32_t expected_node, uint64_t vertices,
                           uint32_t degree, uint32_t remote_degree,
                           bool full_validate, pr_graph *graph)
{
    const pr_template_header_v1 *header = mapping;
    uint64_t local_count = vertices * (degree - remote_degree);
    uint64_t remote_count = vertices * remote_degree;
    uint64_t offsets_bytes = (vertices + 1) * sizeof(uint64_t);

    if (mapping == NULL || graph == NULL || mapping_bytes < sizeof(*header) ||
        header->magic != PR_TEMPLATE_MAGIC ||
        header->version != PR_TEMPLATE_VERSION ||
        header->header_size != sizeof(*header) ||
        header->template_base != (uintptr_t)mapping ||
        header->template_bytes != mapping_bytes ||
        header->node != expected_node || header->vertices != vertices ||
        header->degree != degree || header->remote_degree != remote_degree ||
        header->local_edges_count != local_count ||
        header->remote_in_edges_count != remote_count ||
        header->remote_out_edges_count != remote_count ||
        header->config_fingerprint !=
            pr_graph_config_fingerprint(expected_node, vertices, degree,
                                        remote_degree)) {
        return EILSEQ;
    }
    if (!range_valid(header->local_edges_offset,
                     local_count * sizeof(pr_edge), mapping_bytes) ||
        !range_valid(header->local_offsets_offset, offsets_bytes,
                     mapping_bytes) ||
        !range_valid(header->remote_in_edges_offset,
                     remote_count * sizeof(pr_edge), mapping_bytes) ||
        !range_valid(header->remote_in_offsets_offset, offsets_bytes,
                     mapping_bytes) ||
        !range_valid(header->remote_out_edges_offset,
                     remote_count * sizeof(pr_edge), mapping_bytes) ||
        !range_valid(header->remote_out_offsets_offset, offsets_bytes,
                     mapping_bytes)) {
        return EILSEQ;
    }

    memset(graph, 0, sizeof(*graph));
    graph->node = expected_node;
    graph->vertices = vertices;
    graph->degree = degree;
    graph->remote_degree = remote_degree;
    graph->owner_mapping = mapping;
    graph->owner_bytes = mapping_bytes;
    graph->template_backed = true;
    graph->local_in = (pr_edge_set) {
        .edges = (pr_edge *)((unsigned char *)mapping +
                             header->local_edges_offset),
        .offsets = (uint64_t *)((unsigned char *)mapping +
                                header->local_offsets_offset),
        .count = local_count,
        .edge_bytes = local_count * sizeof(pr_edge),
        .offset_bytes = offsets_bytes,
    };
    graph->remote_in = (pr_edge_set) {
        .edges = (pr_edge *)((unsigned char *)mapping +
                             header->remote_in_edges_offset),
        .offsets = (uint64_t *)((unsigned char *)mapping +
                                header->remote_in_offsets_offset),
        .count = remote_count,
        .edge_bytes = remote_count * sizeof(pr_edge),
        .offset_bytes = offsets_bytes,
    };
    graph->remote_out = (pr_edge_set) {
        .edges = (pr_edge *)((unsigned char *)mapping +
                             header->remote_out_edges_offset),
        .offsets = (uint64_t *)((unsigned char *)mapping +
                                header->remote_out_offsets_offset),
        .count = remote_count,
        .edge_bytes = remote_count * sizeof(pr_edge),
        .offset_bytes = offsets_bytes,
    };
    if (graph->local_in.offsets[vertices] != local_count ||
        graph->remote_in.offsets[vertices] != remote_count ||
        graph->remote_out.offsets[vertices] != remote_count) {
        memset(graph, 0, sizeof(*graph));
        return EILSEQ;
    }
    if (full_validate && pr_graph_checksum(graph) != header->graph_checksum) {
        memset(graph, 0, sizeof(*graph));
        return EILSEQ;
    }
    return 0;
}

static void free_owned_array(void *mapping, uint64_t bytes)
{
    if (mapping != NULL && bytes != 0) {
        (void)munmap(mapping, (size_t)align_up(bytes, PR_HUGE_SIZE));
    }
}

void pr_graph_destroy(pr_graph *graph)
{
    if (graph == NULL) {
        return;
    }
    if (!graph->template_backed) {
        free_owned_array(graph->local_in.edges, graph->local_in.edge_bytes);
        free_owned_array(graph->local_in.offsets,
                         graph->local_in.offset_bytes);
        free_owned_array(graph->remote_in.edges, graph->remote_in.edge_bytes);
        free_owned_array(graph->remote_in.offsets,
                         graph->remote_in.offset_bytes);
        free_owned_array(graph->remote_out.edges,
                         graph->remote_out.edge_bytes);
        free_owned_array(graph->remote_out.offsets,
                         graph->remote_out.offset_bytes);
    }
    memset(graph, 0, sizeof(*graph));
}
