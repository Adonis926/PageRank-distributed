#define _GNU_SOURCE

#include "pr_cxlfork.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#if MR_ENABLE_CXLFORK
#include <linux/cxlfork.h>

_Static_assert(sizeof(struct cxlfork_prepare_v2) == 240,
               "unexpected cxlfork_prepare_v2 ABI size");
#endif

#if MR_ENABLE_CXLFORK
static uint64_t now_ns(void)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
}

static void init_v2(void *ptr, size_t size)
{
    struct cxlfork_v2_header *header = ptr;

    memset(ptr, 0, size);
    header->size = (uint32_t)size;
    header->abi_version = CXLFORK_ABI_VERSION_V2;
}

static int validate_options(const pr_cxlfork_options *options)
{
    if (options == NULL || options->key == 0 ||
        options->base % (2ULL << 20) != 0 ||
        options->bytes < PR_TEMPLATE_HEADER_BYTES ||
        options->bytes % (2ULL << 20) != 0 ||
        options->vertices == 0 || options->vertices > UINT32_MAX ||
        options->remote_degree == 0 ||
        options->remote_degree >= options->degree) {
        return EINVAL;
    }
    return 0;
}

static mr_cxlfork_restore_options make_restore_options(
    const pr_cxlfork_options *options)
{
    return (mr_cxlfork_restore_options) {
        .template = {
            .role = MR_ROLE_REDUCE,
            .key = options->key,
            .base = options->base,
            .bytes = options->bytes,
            .seed = 0,
            .value_scale = 1,
        },
        .generation = options->generation,
        .page_checksum = options->page_checksum,
        .policy = MR_CXLFORK_POLICY_READONLY,
        .validate_full = options->full_validate,
    };
}
#endif

int pr_cxlfork_publish(const pr_cxlfork_options *options)
{
#if MR_ENABLE_CXLFORK
    struct cxlfork_query_caps_v2 caps;
    struct cxlfork_prepare_v2 prepare;
    struct cxlfork_region regions[CXLFORK_MAX_REGIONS];
    pr_graph graph;
    void *mapping = MAP_FAILED;
    int fd = -1;
    int err;
    uint64_t begin_ns;
    uint64_t build_begin_ns;
    uint64_t build_end_ns;
    uint64_t prepare_begin_ns;
    uint64_t prepare_end_ns;

    err = validate_options(options);
    if (err != 0 || options->generation != 0 ||
        options->page_checksum != 0) {
        return err != 0 ? err : EINVAL;
    }
    begin_ns = now_ns();
    fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        return errno;
    }
    init_v2(&caps, sizeof(caps));
    if (ioctl(fd, CXLFORK_QUERY_CAPS_V2, &caps) != 0) {
        err = errno;
        close(fd);
        return err;
    }
    if (!(caps.capabilities & CXLFORK_CAP_V2_ABI) ||
        !(caps.capabilities & CXLFORK_CAP_CONSISTENT_SINGLE_THREAD) ||
        options->bytes > caps.slot_bytes || caps.page_size != 4096) {
        close(fd);
        return EOPNOTSUPP;
    }

    mapping = mmap((void *)(uintptr_t)options->base, (size_t)options->bytes,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE,
                   -1, 0);
    if (mapping == MAP_FAILED) {
        err = errno;
        close(fd);
        return err;
    }
    if (madvise(mapping, (size_t)options->bytes, MADV_NOHUGEPAGE) != 0) {
        err = errno;
        munmap(mapping, (size_t)options->bytes);
        close(fd);
        return err;
    }
    build_begin_ns = now_ns();
    err = pr_graph_build_template(mapping, options->bytes, 1,
                                  options->vertices, options->degree,
                                  options->remote_degree, &graph);
    build_end_ns = now_ns();
    if (err != 0) {
        munmap(mapping, (size_t)options->bytes);
        close(fd);
        return err;
    }

    memset(regions, 0, sizeof(regions));
    init_v2(&prepare, sizeof(prepare));
    prepare.key = options->key;
    prepare.scan_start = options->base;
    prepare.scan_end = options->base + options->bytes;
    prepare.regions_ptr = (uintptr_t)regions;
    prepare.region_capacity = CXLFORK_MAX_REGIONS;
    prepare_begin_ns = now_ns();
    if (ioctl(fd, CXLFORK_PREPARE_V2, &prepare) != 0) {
        err = errno;
        munmap(mapping, (size_t)options->bytes);
        close(fd);
        return err;
    }
    prepare_end_ns = now_ns();
    const pr_template_header_v1 *header = mapping;
    printf("pr_template_v1 key=%" PRIu64 " generation=%" PRIu64
           " slot=%" PRIu64 " base=%" PRIu64 " bytes=%" PRIu64
           " vertices=%" PRIu64 " degree=%u remote_degree=%u"
           " graph_bytes=%" PRIu64 " graph_checksum=%" PRIu64
           " total_pages=%" PRIu64 " page_checksum=%" PRIu64
           " region_count=%u build_ns=%" PRIu64
           " prepare_wrapper_ns=%" PRIu64
           " prepare_ioctl_ns=%" PRIu64
           " prepare_kernel_ns=%" PRIu64
           " prepare_page_copy_ns=%" PRIu64 " verify=ok\n",
           options->key, (uint64_t)prepare.generation,
           (uint64_t)prepare.slot, options->base, options->bytes,
           options->vertices, options->degree, options->remote_degree,
           header->graph_bytes, header->graph_checksum,
           (uint64_t)prepare.total_pages,
           (uint64_t)prepare.page_checksum, prepare.region_count,
           build_end_ns - build_begin_ns,
           prepare_end_ns - begin_ns,
           prepare_end_ns - prepare_begin_ns,
           (uint64_t)prepare.stats.total_ns,
           (uint64_t)prepare.stats.page_copy_ns);

    munmap(mapping, (size_t)options->bytes);
    close(fd);
    return 0;
#else
    (void)options;
    return EOPNOTSUPP;
#endif
}

int pr_cxlfork_context_open(const pr_cxlfork_options *options,
                            pr_cxlfork_handle *handle)
{
#if MR_ENABLE_CXLFORK
    int err = validate_options(options);
    if (err != 0 || handle == NULL || options->generation == 0 ||
        options->page_checksum == 0) {
        return err != 0 ? err : EINVAL;
    }
    memset(handle, 0, sizeof(*handle));
    mr_cxlfork_restore_options restore = make_restore_options(options);
    return mr_cxlfork_worker_context_open(&restore, &handle->context,
                                          &handle->timing);
#else
    (void)options;
    (void)handle;
    return EOPNOTSUPP;
#endif
}

int pr_cxlfork_restore(const pr_cxlfork_options *options,
                       pr_cxlfork_handle *handle, pr_graph *graph)
{
#if MR_ENABLE_CXLFORK
    int err;
    mr_cxlfork_restore_options restore;

    if (handle == NULL || handle->context == NULL || graph == NULL) {
        return EINVAL;
    }
    restore = make_restore_options(options);
    err = mr_cxlfork_restore_begin_preopened(&restore, handle->context,
                                             &handle->session,
                                             &handle->result,
                                             &handle->timing);
    if (err != 0) {
        return err;
    }
    err = pr_graph_bind_template((void *)(uintptr_t)options->base,
                                 options->bytes, 1, options->vertices,
                                 options->degree, options->remote_degree,
                                 options->full_validate, graph);
    if (err != 0) {
        mr_cxlfork_restore_end(handle->session);
        handle->session = NULL;
        return err;
    }
    handle->graph_checksum =
        ((const pr_template_header_v1 *)(uintptr_t)options->base)
            ->graph_checksum;
    return 0;
#else
    (void)options;
    (void)handle;
    (void)graph;
    return EOPNOTSUPP;
#endif
}

int pr_cxlfork_collect(pr_cxlfork_handle *handle)
{
    if (handle == NULL || handle->session == NULL) {
        return EINVAL;
    }
    return mr_cxlfork_restore_collect(handle->session, &handle->result);
}

void pr_cxlfork_close(pr_cxlfork_handle *handle)
{
    if (handle == NULL) {
        return;
    }
    if (handle->session != NULL) {
        mr_cxlfork_restore_end(handle->session);
    }
    if (handle->context != NULL) {
        mr_cxlfork_worker_context_close(handle->context);
    }
    memset(handle, 0, sizeof(*handle));
}
