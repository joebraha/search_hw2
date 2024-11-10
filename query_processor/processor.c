#include "uthash.h" // Include uthash header for hash table
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <strings.h>

#define MAX_WORD_SIZE (size_t)190
#define MAX_TERMS 20
#define HEAP_SIZE 10
#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_BLOCKS                                                             \
    1000 // Maximum number of blocks for one term- need to check this
#define N_DOCUMENTS 8841823 // Number of documents in the collection


// Define constants for search modes
#define CONJUNCTIVE 1
#define DISJUNCTIVE 2

// define structures for min heap to store top 10 results
typedef struct {
    int doc_id;
    double score;
} HeapNode;

typedef struct {
    HeapNode nodes[HEAP_SIZE];
    int size;
} MinHeap;

// define structure for a list pointer
typedef struct {
    char term[MAX_WORD_SIZE];
    int curr_doc_id;     // current posting's docid
    int curr_freq;      // current posting's frequency
    size_t curr_posting; // pointer to current docid in the list
    int curr_block;
    int compressed; // 0 or 1, whether the current docid block is compressed
    int *curr_d_block_uncompressed;
    int *curr_f_block_uncompressed; 
    int num_entries;
} ListPointer;

// Define the structure for lexicon entries
typedef struct {
    char term[MAX_WORD_SIZE];
    int num_entries;
    int start_d_block;
    size_t start_d_offset;
    size_t start_f_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_f_offset;
    int last_did;
    int *last;
    size_t num_blocks; // Number of blocks
    UT_hash_handle hh; // Hash handle for uthash
} LexiconEntry;

// Define the hash table for the lexicon
LexiconEntry *lexicon_table = NULL;

// Define the array for the docs table
int *doc_table = NULL;

// structure to keep track of postings list for a term
typedef struct {
    char term[MAX_WORD_SIZE];
    int num_entries;
    int start_d_block;
    size_t start_d_offset;
    size_t start_f_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_f_offset;
    int last_did;
    int *last;
    size_t num_blocks;
    unsigned char *compressed_d_list;
    unsigned char *compressed_f_list;
    size_t compressed_d_list_size; // do not think we need this
} PostingsList;

// maintaining heap for top 10 results
// swap needed to implement heapify
void swap(HeapNode *a, HeapNode *b) {
    HeapNode temp = *a;
    *a = *b;
    *b = temp;
}

// heapify function to maintain heap property
void heapify(MinHeap *heap, int i) {
    int smallest = i;
    int left = 2 * i + 1;
    int right = 2 * i + 2;

    if (left < heap->size &&
        heap->nodes[left].score < heap->nodes[smallest].score) {
        smallest = left;
    }
    if (right < heap->size &&
        heap->nodes[right].score < heap->nodes[smallest].score) {
        smallest = right;
    }
    if (smallest != i) {
        swap(&heap->nodes[i], &heap->nodes[smallest]);
        heapify(heap, smallest);
    }
}

// function to insert a node into the heap- IF it is greater than the minimum
// value in the heap
void insert(MinHeap *heap, int doc_id, double score) {
    if (heap->size < HEAP_SIZE) {
        // Insert new node at the end
        // if the heap is not yet full, just add the new node
        heap->nodes[heap->size].doc_id = doc_id;
        heap->nodes[heap->size].score = score;
        heap->size++;

        // Heapify up
        int i = heap->size - 1;
        while (i != 0 &&
               heap->nodes[(i - 1) / 2].score > heap->nodes[i].score) {
            swap(&heap->nodes[i], &heap->nodes[(i - 1) / 2]);
            i = (i - 1) / 2;
        }
    } else if (score > heap->nodes[0].score) {
        // Replace root if new score is higher
        // heap is full, insert current node only if it has a higher score than
        // the minimum value in the heap (the root)
        heap->nodes[0].doc_id = doc_id;
        heap->nodes[0].score = score;
        heapify(heap, 0);
    }
}

// Comparison function for qsort
int compare_scores(const void *a, const void *b) {
    float score_a = ((HeapNode *)a)->score;
    float score_b = ((HeapNode *)b)->score;
    return (score_b > score_a) - (score_b < score_a); // Descending order
}

// Function to print the top 10 results
void print_top_10(MinHeap *heap) {
    // Create an array to store the heap elements
    HeapNode sorted_nodes[heap->size];
    for (size_t i = 0; i < heap->size; i++) {
        sorted_nodes[i] = heap->nodes[i];
    }

    // Sort the array by score in descending order
    qsort(sorted_nodes, heap->size, sizeof(HeapNode), compare_scores);

    // Print the sorted array
    printf("Top 10 results:\n");
    for (size_t i = 0; i < heap->size; i++) {
        printf("DocID: %d, Score: %.2f\n", sorted_nodes[i].doc_id, sorted_nodes[i].score);
    }
}

// function to retrieve metadata from lexicon
LexiconEntry *get_metadata(const char *term) {
    LexiconEntry *entry;
    HASH_FIND_STR(lexicon_table, term, entry); // Find entry in hash table
    if (entry == NULL) {
        return NULL;
    }
    return entry;
}

// get compressed postings list from index file for each term in query
size_t retrieve_postings_lists(char **terms, size_t num_terms,
                               PostingsList *postings_lists, FILE *index) {
    size_t valid_terms = 0;
    // printf("Retrieving postings lists for query terms...\n");
    for (size_t i = 0; i < num_terms; i++) {
        // retrieving postings list for term i
        // printf("\tRetrieving metadata for term: %s\n", terms[i]);
        LexiconEntry *metadata = get_metadata(terms[i]);
        if (metadata) {
            // printf("\tMetadata retrieved for term: %s\n", metadata->term);
            // printf("\t\tNum entries: %d\n", metadata->num_entries);
            // printf("\t\tStart d block: %d\n", metadata->start_d_block);
            // printf("\t\tStart d offset: %zu\n", metadata->start_d_offset);
            // printf("\t\tStart f offset: %zu\n", metadata->start_f_offset);
            // printf("\t\tLast d block: %d\n", metadata->last_d_block);
            // printf("\t\tLast d offset: %zu\n", metadata->last_d_offset);
            // printf("\t\tLast f offset: %zu\n", metadata->last_f_offset);
            // printf("\t\tLast did: %d\n", metadata->last_did);
            // printf("\t\tNum blocks: %zu\n", metadata->num_blocks);
            // printf("\t\tLast array:\n");
            // for (size_t j = 0; j < metadata->num_blocks; j++) {
            //     printf("\t\t\tLast[%zu]: %d\n", j, metadata->last[j]);
            // }
            size_t d_start =
                metadata->start_d_offset; // the start offset to be updated as
                                          // we move through blocks
            size_t f_start =
                metadata->start_f_offset; // same but for frequency blocks

            // allocating memory for compressed data
            postings_lists[valid_terms].compressed_d_list =
                malloc(BLOCK_SIZE * metadata->num_blocks);
            postings_lists[valid_terms].compressed_f_list =
                malloc(BLOCK_SIZE * metadata->num_blocks);
            if (!postings_lists[valid_terms].compressed_d_list ||
                !postings_lists[valid_terms].compressed_f_list) {
                perror("Error allocating memory for compressed docid list and "
                       "compressed frequency list");
                exit(EXIT_FAILURE);
            }

            // to keep track of where we are writing to in
            // postings_lists[i].compressed_d_list or postings_lists[i].compressed_f_list
            size_t d_offset = 0;
            size_t f_offset = 0;

            for (int d = metadata->start_d_block; d <= metadata->last_d_block;
                 d += 2) {
                // printf("\t\tFetching compressed d block %d...\n", d);
                // read in the docids and freqs
                int f = d + 1; // frequencies always stored in block after docids
                // printf("\t\tFetching compressed f block %d...\n", f);
                size_t d_block_offset =
                    (d * BLOCK_SIZE) + d_start; // actual location to seek to
                                                // for the start of the docids
                size_t f_block_offset =
                    (f * BLOCK_SIZE) +
                    f_start; // actual location to seek to for start of frequencies
                if (d != metadata->last_d_block) {
                    // not the last block, read in from the offset to the end of
                    // the block
                    // printf("\t\tReading in from d block offset: %zu to end of block\n", d_block_offset);
                    fseek(index, d_block_offset,
                          SEEK_SET); // seek to start of docids in current block
                    fread((postings_lists[valid_terms].compressed_d_list +
                           d_offset),
                          1, (BLOCK_SIZE - d_start),
                          index); // read in from start offset to end of block
                    d_offset +=
                        (BLOCK_SIZE - d_start); // incrementing by size of what
                                                // we just added in
                    d_block_offset +=
                        (BLOCK_SIZE -
                         d_start); // incrementing seek offset by size of what
                                   // we just added in. updating where we start
                                   // reading from in index file
                    d_start = 0; // reset start offset for next block - this is
                                 // where we start reading in next block

                    // printf("\t\tReading in from f block offset: %zu to end of block\n", f_block_offset);
                    fseek(index, f_block_offset,
                          SEEK_SET); // seek to start of frequencies in current block
                    fread((postings_lists[valid_terms].compressed_f_list + f_offset), 1,
                          (BLOCK_SIZE - f_start),
                          index); // read in from start offset to end of block
                    f_offset +=
                        (BLOCK_SIZE - f_start); // incrementing like above
                    f_block_offset +=
                        (BLOCK_SIZE - f_start); // incrementing like above
                    f_start = 0; // reset start offset for next block
                } else {
                    // last or only block- only read in from start offset to
                    // last offset
                    // printf("\t\tLast or only block\n");
                    // printf("\t\tReading in %zu bytes starting from d block offset: %zu and ending at last d offset: %zu\n", (metadata->last_d_offset - d_start), d_block_offset, ((d * BLOCK_SIZE) + metadata->last_d_offset));
                    fseek(index, d_block_offset, SEEK_SET);
                    fread((postings_lists[valid_terms].compressed_d_list +
                           d_offset),
                          1, (metadata->last_d_offset - d_start), index);
                    d_offset += (metadata->last_d_offset - d_start);

                    // printf("\t\tReading in %zu bytes starting from f block offset: %zu and ending at last f offset: %zu\n", (metadata->last_f_offset - f_start), f_block_offset, ((f * BLOCK_SIZE) + metadata->last_f_offset));
                    fseek(index, f_block_offset, SEEK_SET);
                    fread((postings_lists[valid_terms].compressed_f_list + f_offset), 1,
                          (metadata->last_f_offset - f_start), index);
                    f_offset += (metadata->last_f_offset - f_start);
                }
            }

            // Store the postings list and metadata
            strcpy(postings_lists[valid_terms].term, terms[i]);
            postings_lists[valid_terms].num_entries = metadata->num_entries;
            postings_lists[valid_terms].start_d_block = metadata->start_d_block;
            postings_lists[valid_terms].start_d_offset =
                metadata->start_d_offset;
            postings_lists[valid_terms].start_f_offset =
                metadata->start_f_offset;
            postings_lists[valid_terms].last_d_block = metadata->last_d_block;
            postings_lists[valid_terms].last_d_offset = metadata->last_d_offset;
            postings_lists[valid_terms].last_f_offset = metadata->last_f_offset;
            postings_lists[valid_terms].last_did = metadata->last_did;
            postings_lists[valid_terms].compressed_d_list_size = d_offset; // again not sure if needed

            // Copy the last array
            postings_lists[valid_terms].last =
                malloc(sizeof(int) * metadata->num_blocks);
            if (!postings_lists[valid_terms].last) {
                perror("Error allocating memory for last array");
                exit(EXIT_FAILURE);
            }
            memcpy(postings_lists[valid_terms].last, metadata->last,
                   sizeof(int) * metadata->num_blocks);
            postings_lists[valid_terms].num_blocks = metadata->num_blocks;

            // printf("\tPostings list retrieved for term: %s\n", terms[i]);
            // printf("\t\tCompressed d list size: %zu\n", d_offset);
            // printf("\t\tCompressed f list size: %zu\n", f_offset);
            valid_terms++;

        } else {
            printf("Term '%s' not found in lexicon, skipping...\n", terms[i]);
        }
    }
    return valid_terms;
}

// create a list pointer for a postings list
ListPointer *open_list(PostingsList *postings_list) {
    ListPointer *lp = (ListPointer *)malloc(sizeof(ListPointer));
    if (!lp) {
        perror("Error allocating memory for list pointer");
        exit(EXIT_FAILURE);
    }

    strcpy(lp->term, postings_list->term);
    lp->curr_doc_id = -1;
    lp->curr_freq = -1;
    lp->curr_posting = 0;
    lp->curr_block = 0;
    lp->compressed = 1;
    lp->curr_d_block_uncompressed = NULL;
    lp->curr_f_block_uncompressed = NULL;
    lp->num_entries = postings_list->num_entries;
    return lp;
}

// close a list pointer
void close_list(ListPointer *lp) {
    if (lp->curr_d_block_uncompressed) {
        free(lp->curr_d_block_uncompressed);
    }
    if (lp->curr_f_block_uncompressed) {
        free(lp->curr_f_block_uncompressed);
    }
    free(lp);
}

// Comparison function to compare the sizes of the compressed docIDs lists - to
// be used in qsort
int compare_postings_lists(const void *a, const void *b) {
    PostingsList *list_a = (PostingsList *)a;
    PostingsList *list_b = (PostingsList *)b;
    return (list_a->num_entries - list_b->num_entries);
}

size_t varbyte_decode(unsigned char *input, int *output) {
    size_t i = 0;
    int value = 0;
    int shift = 0;

    while (1) {
        unsigned char byte = input[i];
        value |= (byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
        i++;
    }

    *output = value;
    return i + 1; // Return the number of bytes read
}

// get the offset for the current docid block
size_t get_d_block_offset(ListPointer *lp, PostingsList *postings_list) {
    size_t offset;
    if (lp->curr_block == 0) {
        offset = 0;
    } else {
        size_t block_0_size = BLOCK_SIZE - postings_list->start_d_offset;
        offset = block_0_size + ((lp->curr_block - 1) * BLOCK_SIZE);
    }
    return offset;
}

// get the offset for the current frequency block
size_t get_f_block_offset(ListPointer *lp, PostingsList *postings_list) {
    size_t offset;
    if (lp->curr_block == 0) {
        offset = 0;
    } else {
        size_t block_0_size = BLOCK_SIZE - postings_list->start_f_offset;
        offset = block_0_size + ((lp->curr_block - 1) * BLOCK_SIZE);
    }
    return offset;
}

// function to decompress current docid and frequency block
void decompress_block(ListPointer *lp, PostingsList *postings_list) {

    // decompress and write into uncompressed docid block in lp
    size_t offset = get_d_block_offset(lp, postings_list);
    size_t i = 0;
    // printf("\t\t\t\tDecompressing docid block: %d \n", lp->curr_block);
    // printf("\t\t\t\tInitial offset: %zu, Initial i: %zu\n", offset, i);
    // printf("\t\t\t\tCurrent docid block size: %zu\n", lp->curr_d_block_size);
    int last_doc_id_in_block = postings_list->last[lp->curr_block];
    // printf("\t\t\t\tLast docid in block: %d\n", last_doc_id_in_block);
    while (1) {
        size_t bytes_read =
            varbyte_decode(postings_list->compressed_d_list + offset,
                           lp->curr_d_block_uncompressed + i);
        // printf("\t\t\t\t\tBytes read: %zu, Value: %d\n", bytes_read, lp->curr_d_block_uncompressed[i]);
        offset += bytes_read;
        if (lp->curr_d_block_uncompressed[i] == last_doc_id_in_block) {
            i++;
            // printf("\t\t\t\t\tFound last docid in block: %d\n", last_doc_id_in_block);
            break;
        }
        i++;
    }
    // printf("\t\t\t\tTotal docid integers decompressed: %zu\n", i);

    // decompress and write into uncompressed frequency block in lp
    // printf("\t\t\t\tDecompressing frequency block: %d \n", lp->curr_block);
    offset = get_f_block_offset(lp, postings_list);
    // printf("\t\t\t\tReading frequencies starting at offset: %zu\n", offset);
    // i = 0;
    size_t j = 0;
    while (1) {
        size_t bytes_read =
            varbyte_decode(postings_list->compressed_f_list + offset,
                           lp->curr_f_block_uncompressed + j);
        offset += bytes_read;
        j++;
        if (j == i) {
            // need to stop after decoding the number of docids decoded, 
            // because we pad the frequencies block with 0s!!
            // j++;
            break;
        }
    }
    // printf("\t\t\t\tTotal frequencies read: %zu\n", j);

}

// function to get the next greatest or equal docID from a list
int nextGEQ(ListPointer *lp, int k, PostingsList *postings_list) {

    // implement block by block nextGEQ using [last] array
    // printf("\t\t\tDoing block by block search of [last] array to find block containing k: %d, starting at block %d\n", k, lp->curr_block);
    while (postings_list->last[lp->curr_block] < k) {
        // printf("\t\t\t\tCurrent block's last docID: %d is less than k: %d\n", postings_list->last[lp->curr_block], k);
        // printf("\t\t\t\tMoving to next block...\n");
        lp->curr_block++;
        lp->compressed = 1;   // moving to new block, use this info to indicate
                              // that we should free the old uncompressed data
        lp->curr_posting = 0; // reset posting index to 0 for new block
        if (lp->curr_block >= postings_list->num_blocks) {
            // all of the docids in this list are less than k, terminate search
            // we have either hit the end of the list or there are no results to
            // be found
            lp->curr_doc_id = postings_list->last_did;
            return lp->curr_doc_id;
        }
    }

    // at this point, lp->curr_block IS the block that contains the next
    // greatest or equal docID
    // printf("\t\t\tFound block %d with last docID greater than k: %d\n", lp->curr_block, postings_list->last[lp->curr_block]);

    if (lp->compressed) {
        // printf("\t\t\tBlock is compressed.\n");
        // free the old uncompressed data if it exists, make room for new block
        // to be uncompressed
        if (lp->curr_d_block_uncompressed) {
            free(lp->curr_d_block_uncompressed);
            lp->curr_d_block_uncompressed = NULL;
        }
        if (lp->curr_f_block_uncompressed) {
            free(lp->curr_f_block_uncompressed);
            lp->curr_f_block_uncompressed = NULL;
        }

        // printf("\t\t\tAllocating memory for uncompressed blocks...\n");
        size_t max_uncompressed_size =
            BLOCK_SIZE *
            4; // allotting for extra space for uncompressed block of docids
        lp->curr_d_block_uncompressed =
            malloc(max_uncompressed_size * sizeof(int));
        lp->curr_f_block_uncompressed =
            malloc(max_uncompressed_size * sizeof(int));
        if (!lp->curr_d_block_uncompressed || !lp->curr_f_block_uncompressed) {
            perror("Error allocating memory for uncompressed blocks");
            exit(EXIT_FAILURE);
        }
        // printf("\t\t\tDecompressing docid and score blocks...\n");
        decompress_block(lp, postings_list);
        lp->compressed = 0;
    }

    // loop through current decompressed block to find next greatest or equal
    // docID
    // printf("\t\t\tBeginning traversal of decompressed block to find nextGEQ k: %d\n", k);
    int last_doc_id_in_block = postings_list->last[lp->curr_block];
    while (1) {
        // printf("\t\t\t\tChecking docID: %d\n", lp->curr_d_block_uncompressed[lp->curr_posting]);
        if (lp->curr_d_block_uncompressed[lp->curr_posting] >= k) {
            // printf("\t\t\t\tFound nextGEQ docID: %d\n", lp->curr_d_block_uncompressed[lp->curr_posting]);
            lp->curr_doc_id = lp->curr_d_block_uncompressed[lp->curr_posting];
            lp->curr_freq = lp->curr_f_block_uncompressed[lp->curr_posting];
            return lp->curr_doc_id;
        }
        lp->curr_posting++;
    }

    // if we reach here, something has gone wrong
    return -1;
}

// this function loads the document lengths from a file into memory
void load_doc_lengths(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening document lengths file");
        exit(EXIT_FAILURE);
    }

    doc_table = (int *)malloc(N_DOCUMENTS * sizeof(int));

    int doc_id;
    int doc_length;
    while (fscanf(file, "%d %d", &doc_id, &doc_length) == 2) {
        doc_table[doc_id] = doc_length;
    }

    fclose(file);
}

// function to calculate BM25 score of a single word in a document
double get_score(int freq, int doc_id, int num_entries) {
    // printf("\t\t\t\tCalculating BM25 score for docID: %d with frequency %d\n", doc_id, freq);
    double k1 = 1.2;               // free parameter
    double b = 0.75;               // free parameter
    double avg_doc_length = 66.93; // average document length
    int d = doc_table[doc_id];     // length of this document
    // printf("\t\t\t\tDoc length: %d\n", d);

    double score;
    int f = freq; // term frequency in this document
    double tf = 0.0;
    double numerator = f * (k1 + 1.0);
    double denominator = f + k1 * (1.0 - b + b * (d / avg_doc_length));
    tf = numerator / denominator;
    double idf;
    denominator = num_entries + 0.5;
    numerator = N_DOCUMENTS - num_entries + 0.5;
    idf = log((numerator / denominator) + 1.0);
    score = (idf * tf);

    return score;
}


int calculate_score(ListPointer **lp, int num_terms) {
    // printf("\t\t\tCalculating BM25 score for docID: %d\n", lp[0]->curr_doc_id);
    double score = 0;
    for (int i = 0; i < num_terms; i++) {
        // printf("\t\t\t\tTerm: %s, freq: %d, docID: %d, num_entries: %d\n", lp[i]->term, lp[i]->curr_freq, lp[i]->curr_doc_id, lp[i]->num_entries);
        score += get_score(lp[i]->curr_freq, lp[i]->curr_doc_id,
                           lp[i]->num_entries);
        // printf("\t\t\t\tCurrent score: %f\n", score);
    }
    // printf("\t\t\tBM25 score for docID: %d is: %f\n", lp[0]->curr_doc_id, score);
    return score;
}

void c_DAAT(PostingsList *postings_lists, size_t num_terms, MinHeap *top_10) {

    // printf("Starting DAAT retrieval...\n");
    // printf("\tArranging postings lists in order of increasing size of compressed docID lists...\n");

    // step 1 - arrange the lists in order of increasing size of  docID lists
    qsort(postings_lists, num_terms, sizeof(PostingsList),
          compare_postings_lists);

    // step 2 - open all lists, keep array of listpointers in order of shortest
    // to largest docid list
    ListPointer *lp[num_terms];
    int i;
    for (i = 0; i < num_terms; i++) {
        // printf("\tOpening list for term: %s\n", postings_lists[i].term);
        lp[i] = open_list(&postings_lists[i]);
        // printf("\t\tList opened for term: %s\n", lp[i]->term);
    }

    int did = 0;
    int max_did = postings_lists[0]
                      .last_did; // this is the max docID in the shortest list

    // printf("\tMax docID in shortest list: %d\n", max_did);
    // printf("\tStarting traversal...\n");

    // step 3 - traversal
    while (did <= max_did) {
        // get next post from shortest list
        // printf("\n\t\tGetting nextGEQ for docID: %d in shortest list- term %s\n", did, lp[0]->term);
        did = nextGEQ(lp[0], did, &postings_lists[0]);
        // printf("\t\tNextGEQ for shortest list is docID: %d\n", did);
        // check if did is in all other lists, if not, check next greatest docID
        int d;
        size_t j;
        // printf("\t\tChecking if docID is in all other lists...\n");
        for (j = 1; j < num_terms; j++) {
            // printf("\t\t\tLooking for did: %d in term: %s\n", did, lp[j]->term);
            d = nextGEQ(lp[j], did, &postings_lists[j]);
            if (d != did) {
                // printf("\t\t\tDocID: %d not found in term: %s. Break.\n", did, lp[j]->term);
                break;
            }
        }
        if (num_terms == 1) {
            // if there is only one term, then we know that the docID are in
            // "all" of the lists we want to keep going along this list and find
            // the top 10 BM25 scores
            // printf("\t\tOnly one term in query. Continuing search for docID: %d\n", did);
            d = did;
        }
        if (d > did) {
            // we know that the docID is not in all lists
            // check next greatest docID from next list
            // printf("\t\tDocID: %d not found in all lists. Checking next greatest docID: %d.\n", did, d);
            did = d;
        } else if (d < did) {
            // if all the docids in the next shortest list are less than the
            // first element in the shortest list, then we know that there are
            // no documents that contain both terms search terminated early,
            for (i = 0; i < num_terms; i++) {
                // printf("\t\tAll docIDs in next shortest list are less than current docID. Terminating search.\n");
                close_list(lp[i]);
            }
            return;
        } else {
            // we know that the docID is in all lists, or the only list
            // calculate BM25 score
            // printf("\t\tDocID: %d found in all/only list/s. Calculating BM25 score.\n", did);
            double score = calculate_score(lp, num_terms);
            // insert into heap
            // printf("\t\tInserting docID: %d with score: %f into heap.\n", did, score);
            insert(top_10, did, score);
            // printf("\t\tScore inserted into heap. Checking next greatest docID.\n");
            // check next greatest docID from shortest list
            did++;
        }
    }
    // printf("\tTraversal complete.\n");
    // step 4 - close all lists
    for (i = 0; i < num_terms; i++) {
        close_list(lp[i]);
    }
}

int compare_list_pointers(const void *a, const void *b) {
    ListPointer *lp_a = *(ListPointer **)a;
    ListPointer *lp_b = *(ListPointer **)b;
    return (lp_a->curr_doc_id - lp_b->curr_doc_id);
}

int find_lp_with_lowest_doc_id(ListPointer **lp, int num_terms,
                               int max_doc_id) {
    int lowest_index = -1;
    int lowest_doc_id =
        max_doc_id +
        1; // set to max doc id + 1 to ensure we find a lower doc id
    for (int i = 0; i < num_terms; i++) {
        if ((lp[i]->curr_doc_id != -1) &&
            (lp[i]->curr_doc_id < lowest_doc_id)) {
            lowest_doc_id = lp[i]->curr_doc_id;
            lowest_index = i;
        }
    }
    return lowest_index;
}

int ready_to_sum(ListPointer **lp, int num_terms, int did) {
    for (int i = 0; i < num_terms; i++) {
        if ((lp[i]->curr_doc_id != -1) && (lp[i]->curr_doc_id < did)) {
            return 0; // not all lists' current/valid docids are greater than or
                      // equal to did
        }
    }
    return 1; // all lists' current/valid docids are greater than or equal to
              // did
}

void d_DAAT(PostingsList *postings_lists, size_t num_terms, MinHeap *top_10) {

    int greatest_doc_id = postings_lists[0].last_did;
    ListPointer *lp[num_terms];
    int i;
    for (i = 0; i < num_terms; i++) {
        lp[i] = open_list(&postings_lists[i]);
        // get first docID from each list
        lp[i]->curr_doc_id = nextGEQ(lp[i], 0, &postings_lists[i]);
        if (postings_lists[i].last_did > greatest_doc_id) {
            // store greatest doc id out of all terms to limit search
            greatest_doc_id = postings_lists[i].last_did;
        }
    }

    // do qsort on the list pointers based on docID
    // so the first docid in the first list has the lowest docID
    qsort(lp, num_terms, sizeof(ListPointer *), compare_list_pointers);

    int did = lp[0]->curr_doc_id; // start with the lowest docID
    double score;                    // start with the score from the first list
    int lowest_doc_id_index;

    while (1) {
        if (ready_to_sum(lp, num_terms, did)) {
            // all docids are greater than or equal to did, ready to sum up
            // scores
            score = 0; // reset score for new docID
            for (i = 0; i < num_terms; i++) {
                if (lp[i]->curr_doc_id == did) {
                    score += get_score(lp[i]->curr_freq, lp[i]->curr_doc_id, lp[i]->num_entries); // add to score
                    if (lp[i]->curr_doc_id == postings_lists[i].last_did) {
                        lp[i]->curr_doc_id = -1; // no more docIDs in this list
                    }
                }
            }
            // insert score into heap
            insert(top_10, did, score);
        }

        lowest_doc_id_index =
            find_lp_with_lowest_doc_id(lp, num_terms, greatest_doc_id);
        if (lowest_doc_id_index == -1) {
            // no more lists to check
            break;
        }
        did = nextGEQ(lp[lowest_doc_id_index], (did + 1),
                      &postings_lists[lowest_doc_id_index]);
    }

    // printf("\tTraversal complete.\n");
    for (i = 0; i < num_terms; i++) {
        close_list(lp[i]);
    }
}

void free_lexicon() {
    LexiconEntry *current_entry, *tmp;

    HASH_ITER(hh, lexicon_table, current_entry, tmp) {
        HASH_DEL(lexicon_table,
                 current_entry);   // Delete the entry from the hash map
        free(current_entry->last); // Free the dynamically allocated array
        free(current_entry);       // Free the entry itself
    }
}

// load lexicon into memory for easy search of term metadata
void load_lexicon(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening lexicon file");
        exit(EXIT_FAILURE);
    }

    char term[MAX_WORD_SIZE];
    int last_did, num_entries, start_d_block, last_d_block;
    size_t start_d_offset, start_f_offset, last_f_offset, last_d_offset,
        num_blocks;

    while (fscanf(file, "%s %d %d %zu %zu %d %zu %zu %d %zu", term,
                  &num_entries, &start_d_block, &start_d_offset,
                  &start_f_offset, &last_d_block, &last_d_offset,
                  &last_f_offset, &last_did, &num_blocks) == 10) {
        LexiconEntry *entry = (LexiconEntry *)malloc(sizeof(LexiconEntry));
        if (!entry) {
            perror("Error allocating memory for lexicon entry");
            exit(EXIT_FAILURE);
        }
        strcpy(entry->term, term);
        entry->num_entries = num_entries;
        entry->start_d_block = start_d_block;
        entry->start_d_offset = start_d_offset;
        entry->start_f_offset = start_f_offset;
        entry->last_d_block = last_d_block;
        entry->last_d_offset = last_d_offset;
        entry->last_f_offset = last_f_offset;
        entry->last_did = last_did;
        entry->num_blocks = num_blocks + 1;

        // Read the variable part (last array)
        entry->last = (int *)malloc(sizeof(int) * entry->num_blocks);
        if (!entry->last) {
            perror("Error allocating memory for last array");
            free(entry);
            exit(EXIT_FAILURE);
        }

        // Read the variable part (last array)
        for (size_t i = 0; i < entry->num_blocks; i++) {
            if (fscanf(file, "%d", &entry->last[i]) != 1) {
                perror("Error reading last array");
                free(entry->last);
                free(entry);
                exit(EXIT_FAILURE);
            }
        }

        // Add the entry to the hash table
        HASH_ADD_STR(lexicon_table, term, entry);
    }

    fclose(file);
}

// Function to parse and clean a query term
void parse_term(char *term) {
    // src traverses original term
    // dst builds cleaned term
    char *src = term, *dst = term;
    while (*src) {
        if (isalpha((unsigned char)*src)) {
            *dst++ = tolower((unsigned char)*src);
            // checks if character is not alphbetic, makes lowercase, moves to
            // next character
        }
        src++;
    }
    *dst = '\0';
}

int main(int argc, char *argv[]) {
    // read in lexicon into memory in hash table
    load_lexicon("../index_generator/lexicon_out");

    // read document lengths into memory
    load_doc_lengths("docs_out.txt");

    // open index file
    FILE *index = fopen("../index_generator/final_index.dat", "rb");

    char search_mode_input[10];
    char query[1024];

    while (1) {
        // Prompt for search mode
        printf("\nEnter search mode - type 'c' for conjunctive, 'd' for "
               "disjunctive, or 'q' if you want to quit: ");
        if (fgets(search_mode_input, sizeof(search_mode_input), stdin) ==
            NULL) {
            break; // Exit on EOF or error
        }
        search_mode_input[strcspn(search_mode_input, "\n")] =
            '\0'; // Remove newline character

        // Determine search mode
        int search_mode;
        if (strcasecmp(search_mode_input, "c") == 0) {
            search_mode = CONJUNCTIVE;
        } else if (strcasecmp(search_mode_input, "d") == 0) {
            search_mode = DISJUNCTIVE;
        } else if (strcasecmp(search_mode_input, "q") == 0) {
            printf("Quitting the program.\n");
            break; // Exit the loop to quit the program
        } else {
            printf("! Invalid search mode ! Please enter 'c' or 'd'.\n");
            continue;
        }

        // Prompt for query terms
        printf("Enter query: ");
        if (fgets(query, sizeof(query), stdin) == NULL) {
            break; // Exit on EOF or error
        }
        query[strcspn(query, "\n")] = '\0'; // Remove newline character

        // Parse the query into individual terms
        char *terms[MAX_TERMS];
        size_t num_terms = 0;
        char *term =
            strtok(query, " \t\n\r\f\v!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        while (term != NULL && num_terms < MAX_TERMS) {
            parse_term(term);
            if (strlen(term) > 0) {
                terms[num_terms++] = strdup(term);
            }
            term =
                strtok(NULL, " \t\n\r\f\v!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        }

        // Retrieve postings lists for all terms in query
        PostingsList postings_lists[num_terms];
        size_t valid_terms =
            retrieve_postings_lists(terms, num_terms, postings_lists, index);
        if (valid_terms == 0) {
            continue; // Query invalid, try again with new query
        }
        // Perform DAAT traversal
        // initialize top-10 heap
        MinHeap top_10 = {.size = 0};
        printf("Performing search on %zu valid terms...\n", valid_terms);
        if (search_mode == CONJUNCTIVE) {
            c_DAAT(postings_lists, valid_terms, &top_10);
        } else {
            d_DAAT(postings_lists, valid_terms, &top_10);
        }

        // print top 10 results
        print_top_10(&top_10);
        // printf("Top 10 results:\n");
        // for (int i = (top_10.size - 1); i >= 0; i--) {
        //     printf("DocID: %d, Score: %f\n", top_10.nodes[i].doc_id,
        //            top_10.nodes[i].score);
        // }

        // Free allocated memory for terms and postings lists
        for (size_t i = 0; i < valid_terms; i++) {
            free(terms[i]);
            free(postings_lists[i].compressed_d_list);
            free(postings_lists[i].compressed_f_list);
        }
    }

    printf("Goodbye!\n");
    // Close index file
    fclose(index);
    free_lexicon();

    return 0;
}
