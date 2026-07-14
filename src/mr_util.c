#include "mr_util.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *mr_xcalloc(size_t n, size_t size)
{
    void *ptr = calloc(n, size);
    if (ptr == NULL) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }
    return ptr;
}

void *mr_xrealloc(void *ptr, size_t size)
{
    void *next = realloc(ptr, size);
    if (next == NULL) {
        fprintf(stderr, "out of memory\n");
        exit(1);
    }
    return next;
}

char *mr_xstrndup(const char *s, size_t len)
{
    char *out = mr_xcalloc(len + 1, 1);
    memcpy(out, s, len);
    return out;
}

char *mr_xstrdup(const char *s)
{
    return mr_xstrndup(s, strlen(s));
}

uint64_t mr_parse_u64(const char *value, const char *name)
{
    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 0);
    if (errno != 0 || end == NULL || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }
    return (uint64_t)parsed;
}

void byte_buf_reserve(byte_buf *buf, size_t need)
{
    if (need <= buf->cap) {
        return;
    }

    size_t cap = buf->cap ? buf->cap : 1024;
    while (cap < need) {
        cap *= 2;
    }
    buf->data = mr_xrealloc(buf->data, cap);
    buf->cap = cap;
}

void byte_buf_append(byte_buf *buf, const void *data, size_t len)
{
    byte_buf_reserve(buf, buf->len + len + 1);
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    buf->data[buf->len] = '\0';
}

void byte_buf_clear(byte_buf *buf)
{
    free(buf->data);
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}
