#include "uthash.h" // Include uthash header for hash table
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define MAX_WORD_SIZE (size_t)190
#define MAX_TERMS 20
#define HEAP_SIZE 10
#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_BLOCKS                                                             \
    1000 // Maximum number of blocks for one term- need to check this

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
    int curr_score;      // current posting's score
    size_t curr_posting; // pointer to current docid in the list
    int curr_block;
    int compressed; // 0 or 1, whether the current docid block is compressed
    int *curr_d_block_uncompressed;
    int *curr_s_block_uncompressed; // scores are already uncompressed because
                                    // they are always 1 byte or less
} ListPointer;

// Define the structure for lexicon entries
typedef struct {
    char term[MAX_WORD_SIZE];
    int num_entries;
    int start_d_block;
    size_t start_d_offset;
    size_t start_s_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_s_offset;
    int last_did;
    int *last;
    size_t num_blocks; // Number of blocks
    UT_hash_handle hh; // Hash handle for uthash
} LexiconEntry;

// Define the hash table for the lexicon
LexiconEntry *lexicon_table = NULL;

// structure to keep track of postings list for a term
typedef struct {
    char term[MAX_WORD_SIZE];
    int num_entries;
    int start_d_block;
    size_t start_d_offset;
    size_t start_s_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_s_offset;
    int last_did;
    int *last;
    size_t num_blocks;
    unsigned char *compressed_d_list;
    unsigned char *s_list; // already uncompressed bc we never compress it in
                           // the first place- always one byte
    size_t compressed_d_list_size;
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
    for (size_t i = 0; i < num_terms; i++) {
        // retrieving postings list for term i
        LexiconEntry *metadata = get_metadata(terms[i]);
        if (metadata) {
            size_t d_start =
                metadata->start_d_offset; // the start offset to be updated as
                                          // we move through blocks
            size_t s_start =
                metadata->start_s_offset; // same but for score blocks

            // allocating memory for compressed data
            postings_lists[valid_terms].compressed_d_list =
                malloc(BLOCK_SIZE * metadata->num_blocks);
            postings_lists[valid_terms].s_list =
                malloc(BLOCK_SIZE * metadata->num_blocks);
            if (!postings_lists[valid_terms].compressed_d_list ||
                !postings_lists[valid_terms].s_list) {
                perror("Error allocating memory for compressed docid list and "
                       "uncompressed score list");
                exit(EXIT_FAILURE);
            }

            // to keep track of where we are writing to in
            // postings_lists[i].compressed_<d or s>_list
            size_t d_offset = 0;
            size_t s_offset = 0;

            for (int d = metadata->start_d_block; d <= metadata->last_d_block;
                 d += 2) {
                // read in the docids and scores
                int s = d + 1; // scores always stored in block after docids
                size_t d_block_offset =
                    (d * BLOCK_SIZE) + d_start; // actual location to seek to
                                                // for the start of the docids
                size_t s_block_offset =
                    (s * BLOCK_SIZE) +
                    s_start; // actual location to seek to for start of scores
                if (d != metadata->last_d_block) {
                    // not the last block, read in from the offset to the end of
                    // the block
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

                    fseek(index, s_block_offset,
                          SEEK_SET); // seek to start of scores in current block
                    fread((postings_lists[valid_terms].s_list + s_offset), 1,
                          (BLOCK_SIZE - s_start),
                          index); // read in from start offset to end of block
                    s_offset +=
                        (BLOCK_SIZE - s_start); // incrementing like above
                    s_block_offset +=
                        (BLOCK_SIZE - s_start); // incrementing like above
                    s_start = 0; // reset start offset for next block
                } else {
                    // last or only block- only read in from start offset to
                    // last offset
                    fseek(index, d_block_offset, SEEK_SET);
                    fread((postings_lists[valid_terms].compressed_d_list +
                           d_offset),
                          1, (metadata->last_d_offset - d_start), index);
                    d_offset += (metadata->last_d_offset - d_start);

                    fseek(index, s_block_offset, SEEK_SET);
                    fread((postings_lists[valid_terms].s_list + s_offset), 1,
                          (metadata->last_s_offset - s_start), index);
                    s_offset += (metadata->last_s_offset - s_start);
                }
            }

            // Store the postings list and metadata
            strcpy(postings_lists[valid_terms].term, terms[i]);
            postings_lists[valid_terms].num_entries = metadata->num_entries;
            postings_lists[valid_terms].start_d_block = metadata->start_d_block;
            postings_lists[valid_terms].start_d_offset =
                metadata->start_d_offset;
            postings_lists[valid_terms].start_s_offset =
                metadata->start_s_offset;
            postings_lists[valid_terms].last_d_block = metadata->last_d_block;
            postings_lists[valid_terms].last_d_offset = metadata->last_d_offset;
            postings_lists[valid_terms].last_s_offset = metadata->last_s_offset;
            postings_lists[valid_terms].last_did = metadata->last_did;
            postings_lists[valid_terms].compressed_d_list_size = d_offset;

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
    lp->curr_score = -1;
    lp->curr_posting = 0;
    lp->curr_block = 0;
    lp->compressed = 1;
    lp->curr_d_block_uncompressed = NULL;
    lp->curr_s_block_uncompressed = NULL;
    return lp;
}

// close a list pointer
void close_list(ListPointer *lp) {
    if (lp->curr_d_block_uncompressed) {
        free(lp->curr_d_block_uncompressed);
    }
    if (lp->curr_s_block_uncompressed) {
        free(lp->curr_s_block_uncompressed);
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

// get the offset for the current score block
size_t get_s_block_offset(ListPointer *lp, PostingsList *postings_list) {
    size_t offset;
    if (lp->curr_block == 0) {
        offset = 0;
    } else {
        size_t block_0_size = BLOCK_SIZE - postings_list->start_s_offset;
        offset = block_0_size + ((lp->curr_block - 1) * BLOCK_SIZE);
    }
    return offset;
}

// function to decompress current docid and fetch score block
void decompress_block(ListPointer *lp, PostingsList *postings_list) {

    // decompress and write into uncompressed docid block in lp
    size_t offset = get_d_block_offset(lp, postings_list);
    size_t i = 0;
    int last_doc_id_in_block = postings_list->last[lp->curr_block];
    while (1) {
        size_t bytes_read =
            varbyte_decode(postings_list->compressed_d_list + offset,
                           lp->curr_d_block_uncompressed + i);
        offset += bytes_read;
        if (lp->curr_d_block_uncompressed[i] == last_doc_id_in_block) {
            i++;
            break;
        }
        i++;
    }

    // fetch block of scores from postings_list->s_list and store in
    // uncompressed score block in lp
    offset = get_s_block_offset(lp, postings_list);
    for (size_t j = 0; j < i; j++) {
        lp->curr_s_block_uncompressed[j] = postings_list->s_list[offset + j];
    }
}

// function to get the next greatest or equal docID from a list
int nextGEQ(ListPointer *lp, int k, PostingsList *postings_list) {

    // implement block by block nextGEQ using [last] array
    while (postings_list->last[lp->curr_block] < k) {
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
    if (lp->compressed) {
        // free the old uncompressed data if it exists, make room for new block
        // to be uncompressed
        if (lp->curr_d_block_uncompressed) {
            free(lp->curr_d_block_uncompressed);
            lp->curr_d_block_uncompressed = NULL;
        }
        if (lp->curr_s_block_uncompressed) {
            free(lp->curr_s_block_uncompressed);
            lp->curr_s_block_uncompressed = NULL;
        }

        size_t max_uncompressed_size =
            BLOCK_SIZE *
            4; // allotting for extra space for uncompressed block of docids
        lp->curr_d_block_uncompressed =
            malloc(max_uncompressed_size * sizeof(int));
        lp->curr_s_block_uncompressed = malloc(
            BLOCK_SIZE * sizeof(int)); // one block of scores should be 64KB
        if (!lp->curr_d_block_uncompressed || !lp->curr_s_block_uncompressed) {
            perror("Error allocating memory for uncompressed blocks");
            exit(EXIT_FAILURE);
        }

        decompress_block(lp, postings_list);
        lp->compressed = 0;
    }

    // loop through current decompressed block to find next greatest or equal
    // docID
    int last_doc_id_in_block = postings_list->last[lp->curr_block];
    while (1) {
        if (lp->curr_d_block_uncompressed[lp->curr_posting] >= k) {
            lp->curr_doc_id = lp->curr_d_block_uncompressed[lp->curr_posting];
            lp->curr_score = lp->curr_s_block_uncompressed[lp->curr_posting];
            return lp->curr_doc_id;
        }
        lp->curr_posting++;
    }

    // if we reach here, something has gone wrong
    return -1;
}

int get_score(ListPointer **lp, int num_terms) {
    int score = 0;
    for (int i = 0; i < num_terms; i++) {
        score += lp[i]->curr_score;
    }
    return score;
}

void c_DAAT(PostingsList *postings_lists, size_t num_terms, MinHeap *top_10) {

    // step 1 - arrange the lists in order of increasing size of  docID lists
    qsort(postings_lists, num_terms, sizeof(PostingsList),
          compare_postings_lists);

    // step 2 - open all lists, keep array of listpointers in order of shortest
    // to largest docid list
    ListPointer *lp[num_terms];
    int i;
    for (i = 0; i < num_terms; i++) {
        lp[i] = open_list(&postings_lists[i]);
    }

    int did = 0;
    int max_did = postings_lists[0]
                      .last_did; // this is the max docID in the shortest list

    // step 3 - traversal
    while (did <= max_did) {
        // get next post from shortest list
        did = nextGEQ(lp[0], did, &postings_lists[0]);
        // check if did is in all other lists, if not, check next greatest docID
        int d;
        size_t j;
        for (j = 1; j < num_terms; j++) {
            d = nextGEQ(lp[j], did, &postings_lists[j]);
            if (d != did) {
                break;
            }
        }
        if (num_terms == 1) {
            // if there is only one term, then we know that the docID are in
            // "all" of the lists we want to keep going along this list and find
            // the top 10 BM25 scores
            d = did;
        }
        if (d > did) {
            // we know that the docID is not in all lists
            // check next greatest docID from next list
            did = d;
        } else if (d < did) {
            // if all the docids in the next shortest list are less than the
            // first element in the shortest list, then we know that there are
            // no documents that contain both terms search terminated early,
            for (i = 0; i < num_terms; i++) {
                close_list(lp[i]);
            }
            return;
        } else {
            // we know that the docID is in all lists, or the only list
            // calculate BM25 score
            int score = get_score(lp, num_terms);
            // insert into heap
            insert(top_10, did, score);
            // check next greatest docID from shortest list
            did++;
        }
    }
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
    int score;                    // start with the score from the first list
    int lowest_doc_id_index;

    while (1) {
        if (ready_to_sum(lp, num_terms, did)) {
            // all docids are greater than or equal to did, ready to sum up
            // scores
            score = 0; // reset score for new docID
            for (i = 0; i < num_terms; i++) {
                if (lp[i]->curr_doc_id == did) {
                    score += lp[i]->curr_score; // add to score
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

    printf("\tTraversal complete.\n");
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
    size_t start_d_offset, start_s_offset, last_s_offset, last_d_offset,
        num_blocks;

    while (fscanf(file, "%s %d %d %zu %zu %d %zu %zu %d %zu", term,
                  &num_entries, &start_d_block, &start_d_offset,
                  &start_s_offset, &last_d_block, &last_d_offset,
                  &last_s_offset, &last_did, &num_blocks) == 10) {
        LexiconEntry *entry = (LexiconEntry *)malloc(sizeof(LexiconEntry));
        if (!entry) {
            perror("Error allocating memory for lexicon entry");
            exit(EXIT_FAILURE);
        }
        strcpy(entry->term, term);
        entry->num_entries = num_entries;
        entry->start_d_block = start_d_block;
        entry->start_d_offset = start_d_offset;
        entry->start_s_offset = start_s_offset;
        entry->last_d_block = last_d_block;
        entry->last_d_offset = last_d_offset;
        entry->last_s_offset = last_s_offset;
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
    load_lexicon("lexicon_out");

    // open index file
    FILE *index = fopen("final_index.dat", "rb");

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
        printf("Top 10 results:\n");
        for (int i = (top_10.size - 1); i >= 0; i--) {
            printf("DocID: %d, Score: %f\n", top_10.nodes[i].doc_id,
                   top_10.nodes[i].score);
        }

        // Free allocated memory for terms and postings lists
        for (size_t i = 0; i < valid_terms; i++) {
            free(terms[i]);
            free(postings_lists[i].compressed_d_list);
            free(postings_lists[i].s_list);
        }
    }

    printf("Goodbye!\n");
    // Close index file
    fclose(index);
    free_lexicon();

    return 0;
}
