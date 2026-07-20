#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#include <stddef.h>
#include <stdint.h>

#define MR_WORKER_STATE_MAGIC UINT64_C(0x4d52574f524b5631)
#define MR_WORKER_STATE_VERSION 1U

#define MR_WORKER_STATE_F_ASCII_ALNUM (1U << 0)
#define MR_WORKER_STATE_F_UNDERSCORE (1U << 1)
#define MR_WORKER_STATE_F_LOWERCASE (1U << 2)

#define MR_WORKER_ROLE_MAP 1U
#define MR_WORKER_ROLE_REDUCE 2U
#define MR_WORKER_ROLE_BOTH 3U

/*
 * Immutable, versioned application state supplied by the worker template.
 * WordCount deliberately consumes these tables and parameters instead of
 * rebuilding equivalent state from process globals.  This makes successful
 * template activation a prerequisite for task execution while keeping code
 * pages and the node-level RPC context outside the checkpoint.
 */
typedef struct MR_WorkerStateV1 {
    uint64_t magic;
    uint32_t version;
    uint32_t size;
    uint32_t role;
    uint32_t flags;
    uint64_t hash_initial;
    uint64_t hash_multiplier;
    uint32_t value_radix;
    uint32_t value_scale;
    uint8_t word_char[256];
    uint8_t lower[256];
    int8_t digit[256];
    uint64_t checksum;
    uint64_t reserved[4];
} MR_WorkerStateV1;

typedef void (*Mapper)(const char *data, size_t len);
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Reducer)(char *key, Getter get_next, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
const MR_WorkerStateV1 *MR_GetWorkerState(void);
void MR_TaskFail(int error);
int MR_Run(int argc, char *argv[], Mapper map, Reducer reduce,
           Partitioner partition);

#endif
