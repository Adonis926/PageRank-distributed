#include "mr_wire.h"

#include <errno.h>
#include <string.h>

#define MR_MAGIC 0x4d524850U
#define MR_VERSION 1U

byte_buf mr_wire_make(mr_task_kind kind, uint64_t task_id,
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

int mr_wire_parse(const uint8_t *payload, uint32_t len, mr_wire_header *hdr,
                  const char **body, size_t *body_len)
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
