#ifndef MR_UTIL_H
#define MR_UTIL_H

#include <stddef.h>
#include <stdint.h>

typedef struct byte_buf {
    char *data;
    size_t len;
    size_t cap;
} byte_buf;

void *mr_xcalloc(size_t n, size_t size);
void *mr_xrealloc(void *ptr, size_t size);
char *mr_xstrndup(const char *s, size_t len);
char *mr_xstrdup(const char *s);
uint64_t mr_parse_u64(const char *value, const char *name);

void byte_buf_reserve(byte_buf *buf, size_t need);
void byte_buf_append(byte_buf *buf, const void *data, size_t len);
void byte_buf_clear(byte_buf *buf);

#endif
