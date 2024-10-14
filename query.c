#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#define DOCTABLE_SIZE 8841823

int *read_doctable(FILE *fdocs) {
    // for line in doc file,
    int *ret = malloc(DOCTABLE_SIZE);
    int id, count;
    while (fscanf(fdocs, "%d %d\n", &id, &count) != EOF) {
        ret[id] = count;
    }
    return ret;
}

int main() {
    // import doctable
    FILE *fdocs = fopen("doctable", "r");
    if (!fdocs) {
        perror("Error opening doctable");
        exit(EXIT_FAILURE);
    }
    int *doctable = read_doctable(fdocs);
    //
    // import lexicon
    //
    // import index metadata
    //
    // setup cache for blocks?
    //
    //
    // accept queries
    //
    // for each query:
    //    find blocks with words in index
    //    decompress blocks
    //    calculate scores (for conjuntive or disjunctive query)
    //    return top 10
}

// data strucures

typedef struct {
} Lexicon; // should be a Hashmap of word to data (block/index start,
           // block/index end, total word count)

// doctable can just be an array of doc_lengths

// store an array of these for all the blocks
typedef struct {
    size_t num_bytes;  // total number of bytes takes up by this compressed
                       // block
    size_t byte_start; // the byte index in the file where this block starts
} BlockMetaData;
