#include "mapreduce.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>

static void map(const char *data, size_t len)
{
    const MR_WorkerStateV1 *state = MR_GetWorkerState();
    if (state == NULL ||
        (state->role != MR_WORKER_ROLE_MAP &&
         state->role != MR_WORKER_ROLE_BOTH) ||
        (state->flags & (MR_WORKER_STATE_F_ASCII_ALNUM |
                         MR_WORKER_STATE_F_UNDERSCORE |
                         MR_WORKER_STATE_F_LOWERCASE)) !=
            (MR_WORKER_STATE_F_ASCII_ALNUM |
             MR_WORKER_STATE_F_UNDERSCORE |
             MR_WORKER_STATE_F_LOWERCASE)) {
        MR_TaskFail(EILSEQ);
        return;
    }
    char word[1024];
    size_t word_len = 0;

    for (size_t i = 0; i <= len; i++) {
        unsigned char ch = i < len ? (unsigned char)data[i] : 0;
        if (i < len && state->word_char[ch] != 0) {
            if (word_len + 1 < sizeof(word)) {
                word[word_len++] = (char)state->lower[ch];
            }
        } else if (word_len > 0) {
            word[word_len] = '\0';
            MR_Emit(word, "1");
            word_len = 0;
        }
    }
}

static void reduce(char *key, Getter get_next, int partition_number)
{
    const MR_WorkerStateV1 *state = MR_GetWorkerState();
    if (state == NULL ||
        (state->role != MR_WORKER_ROLE_REDUCE &&
         state->role != MR_WORKER_ROLE_BOTH) ||
        state->value_radix < 2 || state->value_radix > 10 ||
        state->value_scale == 0) {
        MR_TaskFail(EILSEQ);
        return;
    }
    uint64_t count = 0;
    char *value = NULL;

    while ((value = get_next(key, partition_number)) != NULL) {
        uint64_t parsed = 0;
        for (const unsigned char *p = (const unsigned char *)value;
             *p != '\0'; p++) {
            int digit = state->digit[*p];
            if (digit < 0 || (uint32_t)digit >= state->value_radix ||
                parsed > (UINT64_MAX - (uint64_t)digit) /
                             state->value_radix) {
                MR_TaskFail(EILSEQ);
                return;
            }
            parsed = parsed * state->value_radix + (uint64_t)digit;
        }
        if (count > UINT64_MAX - parsed) {
            MR_TaskFail(EOVERFLOW);
            return;
        }
        count += parsed;
    }
    if (count > UINT64_MAX / state->value_scale) {
        MR_TaskFail(EOVERFLOW);
        return;
    }
    count *= state->value_scale;

    char count_text[32];
    snprintf(count_text, sizeof(count_text), "%" PRIu64, count);
    MR_Emit(key, count_text);
}

int main(int argc, char *argv[])
{
    return MR_Run(argc, argv, map, reduce, MR_DefaultHashPartition);
}
