#include "mapreduce.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

static void map(const char *data, size_t len)
{
    char word[1024];
    size_t word_len = 0;

    for (size_t i = 0; i <= len; i++) {
        unsigned char ch = i < len ? (unsigned char)data[i] : 0;
        if (i < len && (isalnum(ch) || ch == '_')) {
            if (word_len + 1 < sizeof(word)) {
                word[word_len++] = (char)tolower(ch);
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
    int count = 0;
    char *value = NULL;

    while ((value = get_next(key, partition_number)) != NULL) {
        count += atoi(value);
    }

    char count_text[32];
    snprintf(count_text, sizeof(count_text), "%d", count);
    MR_Emit(key, count_text);
}

int main(int argc, char *argv[])
{
    return MR_Run(argc, argv, map, reduce, MR_DefaultHashPartition);
}
