#define _GNU_SOURCE

#include "mr_cxlfork.h"

#include "mr_util.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#if MR_ENABLE_CXLFORK
#include <linux/cxlfork.h>

_Static_assert(sizeof(struct cxlfork_prepare_v2) == 240,
               "unexpected cxlfork_prepare_v2 ABI size");
_Static_assert(sizeof(struct cxlfork_resume_v2) == 312,
               "unexpected cxlfork_resume_v2 ABI size");
_Static_assert(sizeof(struct cxlfork_release_v2) == 64,
               "unexpected cxlfork_release_v2 ABI size");
_Static_assert(sizeof(struct cxlfork_call_stats_v2) == 272,
               "unexpected cxlfork_call_stats_v2 ABI size");
_Static_assert(sizeof(struct cxlfork_call_stats_v3) == 320,
               "unexpected cxlfork_call_stats_v3 ABI size");
_Static_assert(sizeof(struct cxlfork_query_caps_v2) == 128,
               "unexpected cxlfork_query_caps_v2 ABI size");

struct mr_cxlfork_session {
    int fd;
    bool owns_fd;
    bool v3_stats_supported;
    uint64_t key;
    uint64_t generation;
    uint64_t restore_id;
    uint32_t region_count;
    struct cxlfork_region regions[CXLFORK_MAX_REGIONS];
};

struct mr_cxlfork_worker_context {
    int fd;
    struct cxlfork_query_caps_v2 caps;
    uint64_t device_open_ns;
    uint64_t query_caps_ns;
    uint64_t policy_check_ns;
};
#endif

#define MR_TEMPLATE_ALIGNMENT (2ULL * 1024ULL * 1024ULL)

#if MR_ENABLE_CXLFORK
static uint64_t monotonic_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}
#endif

_Static_assert(sizeof(mr_template_header) == 120,
               "mr template v2 header size must stay stable");
_Static_assert(sizeof(MR_WorkerStateV1) == 856,
               "MR worker state v1 size must stay stable");
_Static_assert(MR_TEMPLATE_STATE_OFFSET >= sizeof(mr_template_header),
               "worker state must not overlap the template header");

const char *mr_role_name(mr_worker_role role)
{
    switch (role) {
    case MR_ROLE_MAP:
        return "map";
    case MR_ROLE_REDUCE:
        return "reduce";
    case MR_ROLE_BOTH:
        return "both";
    default:
        return "unknown";
    }
}

int mr_role_parse(const char *text, mr_worker_role *role)
{
    if (strcmp(text, "map") == 0) {
        *role = MR_ROLE_MAP;
        return 0;
    }
    if (strcmp(text, "reduce") == 0) {
        *role = MR_ROLE_REDUCE;
        return 0;
    }
    if (strcmp(text, "both") == 0) {
        *role = MR_ROLE_BOTH;
        return 0;
    }
    return EINVAL;
}

const char *mr_cxlfork_policy_name(mr_cxlfork_policy policy)
{
    switch (policy) {
    case MR_CXLFORK_POLICY_READONLY:
        return "readonly";
    case MR_CXLFORK_POLICY_MOW:
        return "mow";
    case MR_CXLFORK_POLICY_MOA:
        return "moa";
    case MR_CXLFORK_POLICY_HYBRID:
        return "hybrid";
    default:
        return "unknown";
    }
}

int mr_cxlfork_policy_parse(const char *text, mr_cxlfork_policy *policy)
{
    if (strcmp(text, "readonly") == 0) {
        *policy = MR_CXLFORK_POLICY_READONLY;
    } else if (strcmp(text, "mow") == 0) {
        *policy = MR_CXLFORK_POLICY_MOW;
    } else if (strcmp(text, "moa") == 0) {
        *policy = MR_CXLFORK_POLICY_MOA;
    } else if (strcmp(text, "hybrid") == 0) {
        *policy = MR_CXLFORK_POLICY_HYBRID;
    } else {
        return EINVAL;
    }
    return 0;
}

static uint64_t template_checksum_bytes(const unsigned char *data, size_t len)
{
    uint64_t hash = UINT64_C(1469598103934665603);

    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static uint64_t template_next_pattern(uint64_t *state)
{
    uint64_t value = *state;

    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    return value;
}

static uint64_t template_config_fingerprint(const mr_template_options *opts,
                                            uint64_t state_checksum)
{
    return opts->key ^ opts->base ^ opts->bytes ^ opts->seed ^
           (uint64_t)opts->role ^ ((uint64_t)opts->value_scale << 32) ^
           state_checksum;
}

static void template_build_worker_state(const mr_template_options *opts,
                                        MR_WorkerStateV1 *state)
{
    memset(state, 0, sizeof(*state));
    state->magic = MR_WORKER_STATE_MAGIC;
    state->version = MR_WORKER_STATE_VERSION;
    state->size = sizeof(*state);
    state->role = (uint32_t)opts->role;
    state->flags = MR_WORKER_STATE_F_ASCII_ALNUM |
                   MR_WORKER_STATE_F_UNDERSCORE |
                   MR_WORKER_STATE_F_LOWERCASE;
    state->hash_initial = 5381;
    state->hash_multiplier = 33;
    state->value_radix = 10;
    state->value_scale = opts->value_scale;

    for (size_t i = 0; i < 256; i++) {
        state->lower[i] = (uint8_t)i;
        state->digit[i] = -1;
    }
    for (unsigned int c = '0'; c <= '9'; c++) {
        state->word_char[c] = 1;
        state->digit[c] = (int8_t)(c - '0');
    }
    for (unsigned int c = 'A'; c <= 'Z'; c++) {
        state->word_char[c] = 1;
        state->lower[c] = (uint8_t)(c - 'A' + 'a');
    }
    for (unsigned int c = 'a'; c <= 'z'; c++) {
        state->word_char[c] = 1;
    }
    state->word_char[(unsigned int)'_'] = 1;
    state->checksum = 0;
    state->checksum = template_checksum_bytes((const unsigned char *)state,
                                              sizeof(*state));
}

static int template_options_valid(const mr_template_options *opts)
{
    return opts != NULL && opts->role >= MR_ROLE_MAP &&
           opts->role <= MR_ROLE_BOTH && opts->reserved == 0 &&
           opts->value_scale > 0 && opts->value_scale <= 1024 &&
           opts->bytes >= MR_TEMPLATE_STATE_OFFSET +
                              sizeof(MR_WorkerStateV1) &&
           opts->base % MR_TEMPLATE_ALIGNMENT == 0 &&
           opts->bytes % MR_TEMPLATE_ALIGNMENT == 0;
}

int mr_template_map_local(const mr_template_options *opts, void **mapping_out)
{
    unsigned char *mapping;
    uint64_t state;

    if (!template_options_valid(opts) || mapping_out == NULL) {
        return EINVAL;
    }
    mapping = mmap((void *)(uintptr_t)opts->base, (size_t)opts->bytes,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
    if (mapping == MAP_FAILED) {
        return errno;
    }
    if (madvise(mapping, (size_t)opts->bytes, MADV_NOHUGEPAGE) != 0) {
        int err = errno;
        munmap(mapping, (size_t)opts->bytes);
        return err;
    }

    state = opts->seed ^ ((uint64_t)opts->role << 56);
    for (size_t offset = sizeof(mr_template_header); offset < opts->bytes;) {
        uint64_t value = template_next_pattern(&state);
        size_t chunk = opts->bytes - offset;
        if (chunk > sizeof(value)) {
            chunk = sizeof(value);
        }
        memcpy(mapping + offset, &value, chunk);
        offset += chunk;
    }

    MR_WorkerStateV1 *worker_state =
        (MR_WorkerStateV1 *)(mapping + MR_TEMPLATE_STATE_OFFSET);
    template_build_worker_state(opts, worker_state);

    mr_template_header *header = (mr_template_header *)mapping;
    memset(header, 0, sizeof(*header));
    header->magic = MR_TEMPLATE_MAGIC;
    header->version = MR_TEMPLATE_VERSION;
    header->role = (uint32_t)opts->role;
    header->base = opts->base;
    header->bytes = opts->bytes;
    header->seed = opts->seed;
    header->state_offset = MR_TEMPLATE_STATE_OFFSET;
    header->state_bytes = sizeof(*worker_state);
    header->state_checksum = worker_state->checksum;
    header->required_binding_flags = MR_TEMPLATE_BIND_VALIDATED |
                                     MR_TEMPLATE_BIND_ROLE_MATCH |
                                     MR_TEMPLATE_BIND_APP_CONSUMED |
                                     MR_TEMPLATE_BIND_REQUIRED;
    header->payload_checksum = template_checksum_bytes(
        mapping + sizeof(*header), opts->bytes - sizeof(*header));
    header->config_fingerprint =
        template_config_fingerprint(opts, worker_state->checksum);
    *mapping_out = mapping;
    return 0;
}

int mr_template_bind(const void *mapping, const mr_template_options *opts,
                     mr_worker_role expected_role,
                     const MR_WorkerStateV1 **state_out,
                     mr_template_binding_result *binding)
{
    MR_WorkerStateV1 expected;

    if (mapping == NULL || !template_options_valid(opts) ||
        expected_role < MR_ROLE_MAP || expected_role > MR_ROLE_REDUCE) {
        return EINVAL;
    }
    const mr_template_header *header = mapping;
    if (header->magic != MR_TEMPLATE_MAGIC ||
        header->version != MR_TEMPLATE_VERSION ||
        header->role != (uint32_t)opts->role || header->base != opts->base ||
        header->bytes != opts->bytes || header->seed != opts->seed ||
        header->state_offset != MR_TEMPLATE_STATE_OFFSET ||
        header->state_bytes != sizeof(MR_WorkerStateV1) ||
        header->state_offset + header->state_bytes > header->bytes ||
        header->required_binding_flags !=
            (MR_TEMPLATE_BIND_VALIDATED | MR_TEMPLATE_BIND_ROLE_MATCH |
             MR_TEMPLATE_BIND_APP_CONSUMED | MR_TEMPLATE_BIND_REQUIRED)) {
        return EILSEQ;
    }
    for (size_t i = 0; i < sizeof(header->reserved) /
                               sizeof(header->reserved[0]); i++) {
        if (header->reserved[i] != 0) {
            return EILSEQ;
        }
    }

    const MR_WorkerStateV1 *state =
        (const MR_WorkerStateV1 *)((const unsigned char *)mapping +
                                   header->state_offset);
    template_build_worker_state(opts, &expected);
    if (memcmp(state, &expected, sizeof(expected)) != 0 ||
        header->state_checksum != state->checksum ||
        header->config_fingerprint !=
            template_config_fingerprint(opts, state->checksum)) {
        return EILSEQ;
    }
    if (state->role != MR_ROLE_BOTH && state->role != (uint32_t)expected_role) {
        return EACCES;
    }
    if (state_out != NULL) {
        *state_out = state;
    }
    if (binding != NULL) {
        memset(binding, 0, sizeof(*binding));
        binding->state_checksum = state->checksum;
        binding->state_pages =
            (header->state_offset + header->state_bytes - 1) / 4096 -
            header->state_offset / 4096 + 1;
        binding->flags = MR_TEMPLATE_BIND_VALIDATED |
                         MR_TEMPLATE_BIND_ROLE_MATCH |
                         MR_TEMPLATE_BIND_REQUIRED;
    }
    return 0;
}

int mr_template_validate(const void *mapping, const mr_template_options *opts,
                         uint64_t *payload_checksum)
{
    int err = mr_template_bind(mapping, opts,
                               opts != NULL && opts->role == MR_ROLE_REDUCE
                                   ? MR_ROLE_REDUCE
                                   : MR_ROLE_MAP,
                               NULL, NULL);
    if (err == 0 && payload_checksum != NULL) {
        const mr_template_header *header = mapping;
        *payload_checksum = header->payload_checksum;
    }
    return err;
}

void mr_template_unmap(void *mapping, uint64_t bytes)
{
    if (mapping != NULL && mapping != MAP_FAILED && bytes != 0) {
        (void)munmap(mapping, (size_t)bytes);
    }
}

#if MR_ENABLE_CXLFORK
static void init_v2(void *ptr, size_t size)
{
    struct cxlfork_v2_header *header = ptr;

    memset(ptr, 0, size);
    header->size = (uint32_t)size;
    header->abi_version = CXLFORK_ABI_VERSION_V2;
}

static int query_caps_fd(int fd, struct cxlfork_query_caps_v2 *caps)
{
    const uint64_t required = CXLFORK_CAP_V2_ABI |
                              CXLFORK_CAP_CONSISTENT_SINGLE_THREAD |
                              CXLFORK_CAP_RELIABLE_SLOT_GENERATION |
                              CXLFORK_CAP_RESTORE_FAST_PATH;

    init_v2(caps, sizeof(*caps));
    if (ioctl(fd, CXLFORK_QUERY_CAPS_V2, caps) != 0) {
        return errno;
    }
    if ((caps->capabilities & required) != required) {
        return EOPNOTSUPP;
    }
    if (caps->page_size != 4096 || caps->shared_leaf_alignment != MR_TEMPLATE_ALIGNMENT) {
        return EOPNOTSUPP;
    }
    return 0;
}

static int publish_template(const mr_template_options *opts,
                            bool test_corrupt_state)
{
    struct cxlfork_query_caps_v2 caps;
    struct cxlfork_prepare_v2 prepare;
    struct cxlfork_region regions[CXLFORK_MAX_REGIONS];
    void *wanted = (void *)(uintptr_t)opts->base;
    void *mapping = MAP_FAILED;
    int fd = -1;
    int err;

    if (opts->role != MR_ROLE_MAP && opts->role != MR_ROLE_REDUCE) {
        return EINVAL;
    }
    if (opts->key == 0 || !template_options_valid(opts)) {
        return EINVAL;
    }

    fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        return errno;
    }
    err = query_caps_fd(fd, &caps);
    if (err != 0) {
        close(fd);
        return err;
    }
    if (caps.max_slots == 0 || opts->bytes > caps.slot_bytes) {
        close(fd);
        return E2BIG;
    }

    (void)wanted;
    err = mr_template_map_local(opts, &mapping);
    if (err != 0) {
        close(fd);
        return err;
    }
    if (test_corrupt_state) {
        MR_WorkerStateV1 *state =
            (MR_WorkerStateV1 *)((unsigned char *)mapping +
                                 MR_TEMPLATE_STATE_OFFSET);
        state->lower[(unsigned int)'A'] ^= 1U;
        if (mr_template_validate(mapping, opts, NULL) != EILSEQ) {
            munmap(mapping, (size_t)opts->bytes);
            close(fd);
            return EPROTO;
        }
    }

    memset(regions, 0, sizeof(regions));
    init_v2(&prepare, sizeof(prepare));
    prepare.key = opts->key;
    prepare.scan_start = opts->base;
    prepare.scan_end = opts->base + opts->bytes;
    prepare.regions_ptr = (uintptr_t)regions;
    prepare.region_capacity = CXLFORK_MAX_REGIONS;
    if (ioctl(fd, CXLFORK_PREPARE_V2, &prepare) != 0) {
        err = errno;
        munmap(mapping, (size_t)opts->bytes);
        close(fd);
        return err;
    }

    const mr_template_header *header = mapping;
    printf("mr_template_v2 role=%s key=%" PRIu64
           " generation=%" PRIu64 " slot=%" PRIu64
           " base=%" PRIu64 " bytes=%" PRIu64 " seed=%" PRIu64
           " value_scale=%u state_bytes=%" PRIu64
           " state_checksum=%" PRIu64 " state_valid=%s"
           " total_pages=%" PRIu64 " page_checksum=%" PRIu64
           " payload_checksum=%" PRIu64 " region_count=%u"
           " prepare_total_ns=%" PRIu64 " prepare_path_mask=%" PRIu64
           " prepare_valid_mask=%" PRIu64 " verify=ok\n",
           mr_role_name(opts->role), opts->key, (uint64_t)prepare.generation,
           (uint64_t)prepare.slot, opts->base, opts->bytes, opts->seed,
           opts->value_scale, header->state_bytes, header->state_checksum,
           test_corrupt_state ? "no(test)" : "yes",
           (uint64_t)prepare.total_pages, (uint64_t)prepare.page_checksum,
           header->payload_checksum, prepare.region_count,
           (uint64_t)prepare.stats.total_ns,
           (uint64_t)prepare.stats.path_mask,
           (uint64_t)prepare.stats.valid_mask);

    munmap(mapping, (size_t)opts->bytes);
    close(fd);
    return 0;
}

static uint64_t restore_policy_flags(mr_cxlfork_policy policy)
{
    switch (policy) {
    case MR_CXLFORK_POLICY_READONLY:
        return CXLFORK_F_MAP_READONLY;
    case MR_CXLFORK_POLICY_MOW:
        return CXLFORK_F_MIGRATE_ON_WRITE;
    case MR_CXLFORK_POLICY_MOA:
        return CXLFORK_F_MIGRATE_ON_ACCESS;
    case MR_CXLFORK_POLICY_HYBRID:
        return CXLFORK_F_HYBRID_TIERING;
    default:
        return UINT64_MAX;
    }
}

static int validate_restore_caps(
    const mr_cxlfork_restore_options *options,
    const struct cxlfork_query_caps_v2 *caps)
{
    uint64_t flags = restore_policy_flags(options->policy);

    if (flags == UINT64_MAX) {
        return EINVAL;
    }
    if (!(caps->restore_policy_mask & flags)) {
        return EOPNOTSUPP;
    }
    if (options->policy == MR_CXLFORK_POLICY_MOW &&
        !(caps->capabilities & CXLFORK_CAP_MOW_DATA_COW)) {
        return EOPNOTSUPP;
    }
    if (options->policy == MR_CXLFORK_POLICY_MOA &&
        !(caps->capabilities & CXLFORK_CAP_MOA_DATA_COPY)) {
        return EOPNOTSUPP;
    }
    if (options->policy == MR_CXLFORK_POLICY_HYBRID &&
        !(caps->capabilities & CXLFORK_CAP_A_BIT_HYBRID)) {
        return EOPNOTSUPP;
    }
    return 0;
}

static void copy_restore_stats(mr_cxlfork_result *result,
                               const struct cxlfork_restore_stats_v2 *stats)
{
    result->supported_mask = stats->supported_mask;
    result->valid_mask = stats->valid_mask;
    result->path_mask = stats->path_mask;
    result->kernel_total_ns = stats->total_ns;
    result->lock_wait_ns = stats->lock_wait_ns;
    result->lifecycle_ns = stats->lifecycle_ns;
    result->validation_ns = stats->validation_ns;
    result->vma_create_ns = stats->vma_create_ns;
    result->upper_pt_alloc_ns = stats->upper_pt_alloc_ns;
    result->shared_leaf_attach_ns = stats->shared_leaf_attach_ns;
    result->fallback_reconstruct_ns = stats->fallback_reconstruct_ns;
    result->dirty_prefault_ns = stats->dirty_prefault_ns;
    result->global_restore_ns = stats->global_restore_ns;
    result->rollback_ns = stats->rollback_ns;
    result->kernel_other_ns = stats->other_ns;
    result->faults = stats->faults;
    result->fault_dispatch_ns = stats->fault_dispatch_ns;
    result->leaf_materialize_ns = stats->leaf_materialize_ns;
    result->data_cow_ns = stats->data_cow_ns;
    result->cxl_to_local_copy_ns = stats->cxl_to_local_copy_ns;
    result->shared_leaf_pages = stats->shared_leaf_pages;
    result->fallback_pages = stats->fallback_pages;
    result->local_copy_pages = stats->local_copy_pages;
    result->leaf_copy_pages = stats->leaf_copy_pages;
    result->dirty_prefault_pages = stats->dirty_prefault_pages;
    result->validated_ptes = stats->validated_ptes;
}

static void copy_restore_stats_v3(
    mr_cxlfork_result *result,
    const struct cxlfork_restore_stats_v3 *stats)
{
    result->stats_version = 3;
    result->exclusive_supported_mask = stats->supported_mask;
    result->exclusive_valid_mask = stats->valid_mask;
    result->path_mask = stats->path_mask;
    result->kernel_total_ns = stats->total_ns;
    result->lock_wait_ns = stats->lock_wait_ns;
    result->lifecycle_ns = stats->lifecycle_ns;
    result->manifest_validate_ns = stats->manifest_validate_ns;
    result->image_validate_ns = stats->image_validate_ns;
    result->checksum_validate_ns = stats->checksum_validate_ns;
    result->address_conflict_ns = stats->address_conflict_ns;
    result->vma_create_ns = stats->vma_create_ns;
    result->upper_pt_alloc_ns = stats->upper_pt_alloc_ns;
    result->shared_leaf_attach_exclusive_ns =
        stats->shared_leaf_attach_ns;
    result->pte_image_validate_ns = stats->pte_image_validate_ns;
    result->direct_pte_install_ns = stats->direct_pte_install_ns;
    result->fallback_other_ns = stats->fallback_other_ns;
    result->dirty_prefault_ns = stats->dirty_prefault_ns;
    result->global_restore_ns = stats->global_restore_ns;
    result->kernel_other_ns = stats->other_ns;
    result->faults = stats->faults;
    result->fault_dispatch_ns = stats->fault_dispatch_ns;
    result->leaf_materialize_ns = stats->leaf_materialize_ns;
    result->data_cow_ns = stats->data_cow_ns;
    result->cxl_to_local_copy_ns = stats->cxl_to_local_copy_ns;
    result->shared_leaf_count = stats->shared_leaf_count;
    result->shared_data_pages = stats->shared_data_pages;
    result->shared_leaf_pages = stats->shared_data_pages;
    result->fallback_pages = stats->fallback_pages;
    result->direct_pte_install_pages = stats->direct_pte_install_pages;
    result->local_copy_pages = stats->local_copy_pages;
    result->leaf_copy_pages = stats->leaf_copy_pages;
    result->dirty_prefault_pages = stats->dirty_prefault_pages;
    result->validated_ptes = stats->validated_ptes;
}
#endif

#if MR_ENABLE_CXLFORK
static int restore_begin_internal(
    const mr_cxlfork_restore_options *options,
    mr_cxlfork_worker_context *worker,
    mr_cxlfork_session **session_out,
    mr_cxlfork_result *result,
    mr_cxlfork_timing *timing)
{
    struct cxlfork_query_caps_v2 caps;
    struct cxlfork_resume_v2 resume;
    mr_cxlfork_session *session;
    uint64_t flags;
    uint64_t begin;
    uint64_t wrapper_begin = monotonic_ns();
    int err;

    if (options == NULL || session_out == NULL || result == NULL ||
        options->template.key == 0 || options->generation == 0 ||
        options->template.role == MR_ROLE_BOTH) {
        return EINVAL;
    }
    memset(result, 0, sizeof(*result));
    if (timing != NULL) {
        memset(timing, 0, sizeof(*timing));
    }
    flags = restore_policy_flags(options->policy);
    if (flags == UINT64_MAX) {
        return EINVAL;
    }

    begin = monotonic_ns();
    session = calloc(1, sizeof(*session));
    if (timing != NULL) {
        timing->session_alloc_ns = monotonic_ns() - begin;
    }
    if (session == NULL) {
        return ENOMEM;
    }
    session->fd = -1;
    if (worker != NULL) {
        session->fd = worker->fd;
        session->owns_fd = false;
        caps = worker->caps;
        if (timing != NULL) {
            timing->setup_mode = 1;
            timing->preopen_device_ns = worker->device_open_ns;
            timing->preopen_caps_ns = worker->query_caps_ns;
            timing->preopen_policy_ns = worker->policy_check_ns;
        }
    } else {
        begin = monotonic_ns();
        session->fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
        if (timing != NULL) {
            timing->device_open_ns = monotonic_ns() - begin;
        }
        if (session->fd < 0) {
            err = errno;
            free(session);
            return err;
        }
        session->owns_fd = true;
        begin = monotonic_ns();
        err = query_caps_fd(session->fd, &caps);
        if (timing != NULL) {
            timing->query_caps_ns = monotonic_ns() - begin;
        }
        if (err != 0) {
            close(session->fd);
            free(session);
            return err;
        }
    }
    begin = monotonic_ns();
    err = validate_restore_caps(options, &caps);
    if (timing != NULL) {
        timing->policy_check_ns = monotonic_ns() - begin;
    }
    if (err != 0) {
        if (session->owns_fd) {
            close(session->fd);
        }
        free(session);
        return err;
    }
    session->v3_stats_supported =
        (caps.capabilities & CXLFORK_CAP_EXCLUSIVE_RESTORE_STATS) != 0;

    init_v2(&resume, sizeof(resume));
    resume.key = options->template.key;
    resume.generation = options->generation;
    resume.flags = flags;
    if (options->validate_full) {
        resume.flags |= CXLFORK_F_VALIDATE_FULL;
    }
    resume.regions_ptr = (uintptr_t)session->regions;
    resume.region_capacity = CXLFORK_MAX_REGIONS;
    begin = monotonic_ns();
    if (ioctl(session->fd, CXLFORK_RESUME_V2, &resume) != 0) {
        err = errno;
        if (timing != NULL) {
            timing->resume_ioctl_ns = monotonic_ns() - begin;
            timing->wrapper_total_ns = monotonic_ns() - wrapper_begin;
        }
        if (session->owns_fd) {
            close(session->fd);
        }
        free(session);
        return err;
    }
    if (timing != NULL) {
        timing->resume_ioctl_ns = monotonic_ns() - begin;
    }
    begin = monotonic_ns();
    session->key = options->template.key;
    session->generation = options->generation;
    session->restore_id = resume.restore_id;
    session->region_count = resume.region_count;
    result->restore_id = resume.restore_id;
    result->total_pages = resume.total_pages;
    result->page_checksum = resume.page_checksum;
    copy_restore_stats(result, &resume.stats);
    if (resume.generation != options->generation || resume.region_count == 0 ||
        resume.page_checksum != options->page_checksum ||
        resume.total_pages != options->template.bytes / 4096) {
        if (timing != NULL) {
            timing->postcheck_ns = monotonic_ns() - begin;
            timing->wrapper_total_ns = monotonic_ns() - wrapper_begin;
        }
        mr_cxlfork_restore_end(session);
        return EILSEQ;
    }
    if (timing != NULL) {
        timing->postcheck_ns = monotonic_ns() - begin;
        timing->wrapper_total_ns = monotonic_ns() - wrapper_begin;
    }
    *session_out = session;
    return 0;
}
#endif

int mr_cxlfork_worker_context_open(
    const mr_cxlfork_restore_options *options,
    mr_cxlfork_worker_context **context_out,
    mr_cxlfork_timing *timing)
{
#if MR_ENABLE_CXLFORK
    mr_cxlfork_worker_context *context;
    uint64_t begin;
    int err;

    if (options == NULL || context_out == NULL) {
        return EINVAL;
    }
    if (timing != NULL) {
        memset(timing, 0, sizeof(*timing));
        timing->setup_mode = 1;
    }
    context = calloc(1, sizeof(*context));
    if (context == NULL) {
        return ENOMEM;
    }
    context->fd = -1;
    begin = monotonic_ns();
    context->fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    context->device_open_ns = monotonic_ns() - begin;
    if (context->fd < 0) {
        err = errno;
        free(context);
        return err;
    }
    begin = monotonic_ns();
    err = query_caps_fd(context->fd, &context->caps);
    context->query_caps_ns = monotonic_ns() - begin;
    if (err == 0) {
        begin = monotonic_ns();
        err = validate_restore_caps(options, &context->caps);
        context->policy_check_ns = monotonic_ns() - begin;
    }
    if (err != 0) {
        close(context->fd);
        free(context);
        return err;
    }
    if (timing != NULL) {
        timing->preopen_device_ns = context->device_open_ns;
        timing->preopen_caps_ns = context->query_caps_ns;
        timing->preopen_policy_ns = context->policy_check_ns;
    }
    *context_out = context;
    return 0;
#else
    (void)options;
    (void)context_out;
    (void)timing;
    return EOPNOTSUPP;
#endif
}

void mr_cxlfork_worker_context_close(mr_cxlfork_worker_context *context)
{
#if MR_ENABLE_CXLFORK
    if (context != NULL) {
        if (context->fd >= 0) {
            close(context->fd);
        }
        free(context);
    }
#else
    (void)context;
#endif
}

int mr_cxlfork_restore_begin_preopened(
    const mr_cxlfork_restore_options *options,
    mr_cxlfork_worker_context *context,
    mr_cxlfork_session **session_out,
    mr_cxlfork_result *result,
    mr_cxlfork_timing *timing)
{
#if MR_ENABLE_CXLFORK
    if (context == NULL) {
        return EINVAL;
    }
    return restore_begin_internal(options, context, session_out, result,
                                  timing);
#else
    (void)options;
    (void)context;
    (void)session_out;
    (void)result;
    (void)timing;
    return EOPNOTSUPP;
#endif
}

int mr_cxlfork_restore_begin(const mr_cxlfork_restore_options *options,
                             mr_cxlfork_session **session_out,
                             mr_cxlfork_result *result,
                             mr_cxlfork_timing *timing)
{
#if MR_ENABLE_CXLFORK
    return restore_begin_internal(options, NULL, session_out, result, timing);
#else
    (void)options;
    (void)session_out;
    (void)result;
    (void)timing;
    return EOPNOTSUPP;
#endif
}

int mr_cxlfork_restore_collect(mr_cxlfork_session *session,
                               mr_cxlfork_result *result)
{
#if MR_ENABLE_CXLFORK
    if (session == NULL || result == NULL) {
        return EINVAL;
    }
    if (session->v3_stats_supported) {
        struct cxlfork_call_stats_v3 call = {
            .size = sizeof(call),
            .abi_version = CXLFORK_ABI_VERSION_V3,
            .key = session->key,
            .generation = session->generation,
            .restore_id = session->restore_id,
        };

        if (ioctl(session->fd, CXLFORK_GET_CALL_STATS_V3, &call) != 0) {
            return errno;
        }
        copy_restore_stats_v3(result, &call.stats);
    } else {
        struct cxlfork_call_stats_v2 call;

        init_v2(&call, sizeof(call));
        call.key = session->key;
        call.generation = session->generation;
        call.restore_id = session->restore_id;
        if (ioctl(session->fd, CXLFORK_GET_CALL_STATS_V2, &call) != 0) {
            return errno;
        }
        copy_restore_stats(result, &call.stats);
    }
    result->restore_id = session->restore_id;
    return 0;
#else
    (void)session;
    (void)result;
    return EOPNOTSUPP;
#endif
}

void mr_cxlfork_restore_end(mr_cxlfork_session *session)
{
#if MR_ENABLE_CXLFORK
    if (session == NULL) {
        return;
    }
    for (uint32_t i = session->region_count; i > 0; i--) {
        const struct cxlfork_region *region = &session->regions[i - 1];
        if (region->length != 0) {
            (void)munmap((void *)(uintptr_t)region->start,
                         (size_t)region->length);
        }
    }
    if (session->owns_fd) {
        close(session->fd);
    }
    free(session);
#else
    (void)session;
#endif
}

int mr_cxlfork_caps_main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
#if MR_ENABLE_CXLFORK
    struct cxlfork_query_caps_v2 caps;
    int fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        perror(CXLFORK_DEVICE);
        return 1;
    }
    int err = query_caps_fd(fd, &caps);
    close(fd);
    if (err != 0) {
        errno = err;
        perror("CXLFORK_QUERY_CAPS_V2");
        return 1;
    }
    printf("mr_cxlfork_caps_v1 abi=%u capabilities=%" PRIu64
           " cxl_phys=%" PRIu64 " cxl_size=%" PRIu64
           " slot_bytes=%" PRIu64 " max_slots=%u max_regions=%u"
           " max_ptes=%u descriptor_version=%u page_size=%u"
           " shared_leaf_alignment=%" PRIu64 " policy_mask=%" PRIu64
           " verify=ok\n",
           caps.abi_version, (uint64_t)caps.capabilities,
           (uint64_t)caps.cxl_phys, (uint64_t)caps.cxl_size,
           (uint64_t)caps.slot_bytes, caps.max_slots, caps.max_regions, caps.max_ptes,
           caps.descriptor_version, caps.page_size,
           (uint64_t)caps.shared_leaf_alignment,
           (uint64_t)caps.restore_policy_mask);
    return 0;
#else
    fprintf(stderr, "mr_hydra was built without MR_ENABLE_CXLFORK\n");
    return 2;
#endif
}

int mr_cxlfork_template_main(int argc, char **argv)
{
#if MR_ENABLE_CXLFORK
    mr_template_options opts = {
        .base = MR_TEMPLATE_BASE_DEFAULT,
        .bytes = MR_TEMPLATE_BYTES_DEFAULT,
        .seed = MR_TEMPLATE_SEED_DEFAULT,
        .value_scale = MR_TEMPLATE_VALUE_SCALE_DEFAULT,
    };
    bool test_corrupt_state = false;

    for (int i = 2; i < argc; i++) {
        const char *arg = argv[i];
        const char *value;
        if (strncmp(arg, "--role=", 7) == 0) {
            value = arg + 7;
            if (mr_role_parse(value, &opts.role) != 0 || opts.role == MR_ROLE_BOTH) {
                fprintf(stderr, "template role must be map or reduce\n");
                return 2;
            }
        } else if (strncmp(arg, "--key=", 6) == 0) {
            opts.key = mr_parse_u64(arg + 6, "key");
        } else if (strncmp(arg, "--base=", 7) == 0) {
            opts.base = mr_parse_u64(arg + 7, "base");
        } else if (strncmp(arg, "--bytes=", 8) == 0) {
            opts.bytes = mr_parse_u64(arg + 8, "bytes");
        } else if (strncmp(arg, "--seed=", 7) == 0) {
            opts.seed = mr_parse_u64(arg + 7, "seed");
        } else if (strncmp(arg, "--value-scale=", 14) == 0) {
            uint64_t scale = mr_parse_u64(arg + 14, "value-scale");
            if (scale == 0 || scale > 1024) {
                fprintf(stderr, "value-scale must be in [1, 1024]\n");
                return 2;
            }
            opts.value_scale = (uint32_t)scale;
        } else if (strcmp(arg, "--test-corrupt-state") == 0) {
            test_corrupt_state = true;
        } else {
            fprintf(stderr, "unknown template arg: %s\n", arg);
            return 2;
        }
    }
    if (opts.role == 0 || opts.key == 0) {
        fprintf(stderr, "template requires --role=map|reduce and --key=N\n");
        return 2;
    }
    int err = publish_template(&opts, test_corrupt_state);
    if (err != 0) {
        errno = err;
        perror("mr template publish");
        return 1;
    }
    return 0;
#else
    (void)argc;
    (void)argv;
    fprintf(stderr, "mr_hydra was built without MR_ENABLE_CXLFORK\n");
    return 2;
#endif
}

int mr_cxlfork_inspect_main(int argc, char **argv)
{
#if MR_ENABLE_CXLFORK
    struct cxlfork_inspect_v2 inspect;
    if (argc != 3) {
        fprintf(stderr, "usage: %s cxlfork-inspect KEY\n", argv[0]);
        return 2;
    }
    uint64_t key = mr_parse_u64(argv[2], "key");
    int fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        perror(CXLFORK_DEVICE);
        return 1;
    }
    init_v2(&inspect, sizeof(inspect));
    inspect.key = key;
    if (ioctl(fd, CXLFORK_INSPECT_V2, &inspect) != 0) {
        int err = errno;
        close(fd);
        errno = err;
        perror("CXLFORK_INSPECT_V2");
        return 1;
    }
    close(fd);
    printf("mr_cxlfork_inspect_v1 key=%" PRIu64
           " generation=%" PRIu64 " slot=%u state=%u readers=%u"
           " pages=%" PRIu64 " page_checksum=%" PRIu64
           " observed_checksum=%" PRIu64 " flags=%" PRIu64
           " verify=%s\n",
           key, (uint64_t)inspect.generation, inspect.slot, inspect.state,
           inspect.reader_count, (uint64_t)inspect.total_pages,
           (uint64_t)inspect.page_checksum,
           (uint64_t)inspect.observed_page_checksum,
           (uint64_t)inspect.flags,
           inspect.page_checksum == inspect.observed_page_checksum &&
                   !(inspect.flags & CXLFORK_INSPECT_F_CHECKSUM_MISMATCH)
               ? "ok"
               : "fail");
    return 0;
#else
    (void)argc;
    (void)argv;
    fprintf(stderr, "mr_hydra was built without MR_ENABLE_CXLFORK\n");
    return 2;
#endif
}

int mr_cxlfork_release_main(int argc, char **argv)
{
#if MR_ENABLE_CXLFORK
    struct cxlfork_release_v2 release;
    if (argc != 4) {
        fprintf(stderr, "usage: %s cxlfork-release KEY GENERATION\n", argv[0]);
        return 2;
    }
    init_v2(&release, sizeof(release));
    release.key = mr_parse_u64(argv[2], "key");
    release.generation = mr_parse_u64(argv[3], "generation");
    int fd = open(CXLFORK_DEVICE, O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        perror(CXLFORK_DEVICE);
        return 1;
    }
    if (ioctl(fd, CXLFORK_RELEASE_V2, &release) != 0) {
        int err = errno;
        close(fd);
        errno = err;
        perror("CXLFORK_RELEASE_V2");
        return 1;
    }
    close(fd);
    printf("mr_cxlfork_release_v1 key=%" PRIu64
           " generation=%" PRIu64 " verify=ok\n",
           (uint64_t)release.key, (uint64_t)release.generation);
    return 0;
#else
    (void)argc;
    (void)argv;
    fprintf(stderr, "mr_hydra was built without MR_ENABLE_CXLFORK\n");
    return 2;
#endif
}
