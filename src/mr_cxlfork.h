#ifndef MR_CXLFORK_H
#define MR_CXLFORK_H

#include <stddef.h>
#include <stdint.h>

#define MR_TEMPLATE_MAGIC UINT64_C(0x4d52544d504c5631)
#define MR_TEMPLATE_VERSION 1U
#define MR_TEMPLATE_BASE_DEFAULT UINT64_C(0x500000000000)
#define MR_TEMPLATE_BYTES_DEFAULT (64ULL * 1024ULL * 1024ULL)
#define MR_TEMPLATE_SEED_DEFAULT UINT64_C(0x43584c464f524b31)

typedef enum mr_worker_role {
    MR_ROLE_MAP = 1,
    MR_ROLE_REDUCE = 2,
    MR_ROLE_BOTH = 3,
} mr_worker_role;

typedef struct mr_template_header {
    uint64_t magic;
    uint32_t version;
    uint32_t role;
    uint64_t base;
    uint64_t bytes;
    uint64_t seed;
    uint64_t payload_checksum;
    uint64_t config_fingerprint;
    uint64_t reserved[8];
} mr_template_header;

typedef struct mr_template_options {
    mr_worker_role role;
    uint64_t key;
    uint64_t base;
    uint64_t bytes;
    uint64_t seed;
} mr_template_options;

const char *mr_role_name(mr_worker_role role);
int mr_role_parse(const char *text, mr_worker_role *role);
int mr_cxlfork_caps_main(int argc, char **argv);
int mr_cxlfork_template_main(int argc, char **argv);
int mr_cxlfork_inspect_main(int argc, char **argv);
int mr_cxlfork_release_main(int argc, char **argv);

#endif
