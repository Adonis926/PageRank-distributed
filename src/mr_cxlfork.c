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
_Static_assert(sizeof(struct cxlfork_query_caps_v2) == 128,
               "unexpected cxlfork_query_caps_v2 ABI size");
#endif

#define MR_TEMPLATE_ALIGNMENT (2ULL * 1024ULL * 1024ULL)

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

static uint64_t checksum_bytes(const unsigned char *data, size_t len)
{
    uint64_t hash = UINT64_C(1469598103934665603);

    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static uint64_t next_pattern(uint64_t *state)
{
    uint64_t value = *state;

    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value;
    return value;
}

static int fill_template(void *mapping, const mr_template_options *opts)
{
    unsigned char *bytes = mapping;
    uint64_t state = opts->seed ^ ((uint64_t)opts->role << 56);

    for (size_t offset = sizeof(mr_template_header); offset < opts->bytes;) {
        uint64_t value = next_pattern(&state);
        size_t chunk = opts->bytes - offset;
        if (chunk > sizeof(value)) {
            chunk = sizeof(value);
        }
        memcpy(bytes + offset, &value, chunk);
        offset += chunk;
    }

    mr_template_header *header = mapping;
    memset(header, 0, sizeof(*header));
    header->magic = MR_TEMPLATE_MAGIC;
    header->version = MR_TEMPLATE_VERSION;
    header->role = (uint32_t)opts->role;
    header->base = opts->base;
    header->bytes = opts->bytes;
    header->seed = opts->seed;
    header->payload_checksum =
        checksum_bytes(bytes + sizeof(*header), opts->bytes - sizeof(*header));
    header->config_fingerprint = opts->key ^ opts->base ^ opts->bytes ^
                                 opts->seed ^ (uint64_t)opts->role;
    return 0;
}

static int publish_template(const mr_template_options *opts)
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
    if (opts->key == 0 || opts->bytes < sizeof(mr_template_header) ||
        opts->base % MR_TEMPLATE_ALIGNMENT != 0 ||
        opts->bytes % MR_TEMPLATE_ALIGNMENT != 0) {
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

    mapping = mmap(wanted, (size_t)opts->bytes, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
    if (mapping == MAP_FAILED) {
        err = errno;
        close(fd);
        return err;
    }
    if (madvise(mapping, (size_t)opts->bytes, MADV_NOHUGEPAGE) != 0) {
        err = errno;
        munmap(mapping, (size_t)opts->bytes);
        close(fd);
        return err;
    }
    fill_template(mapping, opts);

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
    printf("mr_template_v1 role=%s key=%" PRIu64
           " generation=%" PRIu64 " slot=%" PRIu64
           " base=%" PRIu64 " bytes=%" PRIu64 " seed=%" PRIu64
           " total_pages=%" PRIu64 " page_checksum=%" PRIu64
           " payload_checksum=%" PRIu64 " region_count=%u"
           " prepare_total_ns=%" PRIu64 " prepare_path_mask=%" PRIu64
           " prepare_valid_mask=%" PRIu64 " verify=ok\n",
           mr_role_name(opts->role), opts->key, (uint64_t)prepare.generation,
           (uint64_t)prepare.slot, opts->base, opts->bytes, opts->seed,
           (uint64_t)prepare.total_pages, (uint64_t)prepare.page_checksum,
           header->payload_checksum, prepare.region_count,
           (uint64_t)prepare.stats.total_ns,
           (uint64_t)prepare.stats.path_mask,
           (uint64_t)prepare.stats.valid_mask);

    munmap(mapping, (size_t)opts->bytes);
    close(fd);
    return 0;
}
#endif

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
    };

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
        } else {
            fprintf(stderr, "unknown template arg: %s\n", arg);
            return 2;
        }
    }
    if (opts.role == 0 || opts.key == 0) {
        fprintf(stderr, "template requires --role=map|reduce and --key=N\n");
        return 2;
    }
    int err = publish_template(&opts);
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
