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

typedef enum mr_cxlfork_policy {
    MR_CXLFORK_POLICY_READONLY = 0,
    MR_CXLFORK_POLICY_MOW = 1,
    MR_CXLFORK_POLICY_MOA = 2,
    MR_CXLFORK_POLICY_HYBRID = 3,
} mr_cxlfork_policy;

typedef struct mr_cxlfork_restore_options {
    mr_template_options template;
    uint64_t generation;
    uint64_t page_checksum;
    mr_cxlfork_policy policy;
    int validate_full;
} mr_cxlfork_restore_options;

typedef struct mr_cxlfork_result {
    uint64_t restore_id;
    uint64_t total_pages;
    uint64_t page_checksum;
    uint64_t supported_mask;
    uint64_t valid_mask;
    uint64_t path_mask;
    uint64_t kernel_total_ns;
    uint64_t lock_wait_ns;
    uint64_t lifecycle_ns;
    uint64_t validation_ns;
    uint64_t vma_create_ns;
    uint64_t upper_pt_alloc_ns;
    uint64_t shared_leaf_attach_ns;
    uint64_t fallback_reconstruct_ns;
    uint64_t dirty_prefault_ns;
    uint64_t global_restore_ns;
    uint64_t rollback_ns;
    uint64_t kernel_other_ns;
    uint64_t faults;
    uint64_t fault_dispatch_ns;
    uint64_t leaf_materialize_ns;
    uint64_t data_cow_ns;
    uint64_t cxl_to_local_copy_ns;
    uint64_t shared_leaf_pages;
    uint64_t fallback_pages;
    uint64_t local_copy_pages;
    uint64_t leaf_copy_pages;
    uint64_t dirty_prefault_pages;
    uint64_t validated_ptes;
} mr_cxlfork_result;

typedef struct mr_cxlfork_session mr_cxlfork_session;

const char *mr_role_name(mr_worker_role role);
int mr_role_parse(const char *text, mr_worker_role *role);
const char *mr_cxlfork_policy_name(mr_cxlfork_policy policy);
int mr_cxlfork_policy_parse(const char *text, mr_cxlfork_policy *policy);
int mr_template_map_local(const mr_template_options *opts, void **mapping_out);
int mr_template_validate(const void *mapping, const mr_template_options *opts,
                         uint64_t *payload_checksum);
void mr_template_unmap(void *mapping, uint64_t bytes);
int mr_cxlfork_restore_begin(const mr_cxlfork_restore_options *options,
                             mr_cxlfork_session **session,
                             mr_cxlfork_result *result);
int mr_cxlfork_restore_collect(mr_cxlfork_session *session,
                               mr_cxlfork_result *result);
void mr_cxlfork_restore_end(mr_cxlfork_session *session);
int mr_cxlfork_caps_main(int argc, char **argv);
int mr_cxlfork_template_main(int argc, char **argv);
int mr_cxlfork_inspect_main(int argc, char **argv);
int mr_cxlfork_release_main(int argc, char **argv);

#endif
