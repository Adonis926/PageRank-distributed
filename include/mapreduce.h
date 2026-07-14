#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#include <stddef.h>

typedef void (*Mapper)(const char *data, size_t len);
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Reducer)(char *key, Getter get_next, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
int MR_Run(int argc, char *argv[], Mapper map, Reducer reduce,
           Partitioner partition);

#endif
