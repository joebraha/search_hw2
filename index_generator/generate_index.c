#include "uthash.h" // Include uthash
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)190
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024) // 128MB
#define MAX_BLOCKS 1000 // Maximum number of blocks for one term- overestimating
// #define N_DOCUMENTS 8841823 // Number of documents in the collection

typedef struct {
    size_t size;
    unsigned char *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char *term;
    int count;
    int num_entries;       // number of documents containing this word
    int start_d_block;     // The block number where the first did resides
    size_t start_d_offset; // Offset within the block where the first docID is
                           // stored
    size_t start_f_offset; // Offset within the block where the first frequency
                           // is stored
    int last_d_block;      // the block number where the last did resides
    size_t
        last_d_offset; // Offset within the block where the last docID is stored
    size_t last_f_offset; // Offset within the block where the last frequency
                          // is stored
    int last_did;         // the last docID of the term
    int *last;            // Array to store the last docID in each block
    size_t num_blocks; // Number of d blocks that the term's posting list spans
} LexiconEntry;

typedef struct {
    size_t size;
    unsigned char *data;
} CompressedData;

typedef struct {
    char *term;        // Key
    int count;         // Value
    UT_hash_handle hh; // Makes this structure hashable
} TermEntry;

TermEntry *terms = NULL; // Hash table

// this function reads the words_out file and populates the terms hash table
// with the terms and their counts
void read_words_out(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open words_out.txt");
        exit(EXIT_FAILURE);
    }

    char term[256];
    int count;
    while (fscanf(file, "%s %d", term, &count) == 2) {
        TermEntry *entry = malloc(sizeof(TermEntry));
        entry->term = strdup(term);
        entry->count = count;
        HASH_ADD_KEYPTR(hh, terms, entry->term, strlen(entry->term), entry);
    }

    fclose(file);
}

// this function writes the all of the blocks in memory to the index file on
// disc
void pipe_to_file(MemoryBlock *blocks, FILE *file) {
    // Ensure the file is open
    if (!file) {
        perror("Error: File is not open");
        return;
    }

    // Write the data to the file
    size_t written = fwrite(blocks->data, 1, blocks->size, file);
    if (written != blocks->size) {
        perror("Error writing blocks to file");
        exit(EXIT_FAILURE);
    }

    // Reset the blocks size
    blocks->size = 0;
}


// this function takes in a memory block structure and either adds
// it to the blocks array in memory or writes all of the blocks
// to disc to make room for the new block
void add_to_index(MemoryBlock *block, int *current_block_number,
                  MemoryBlock *blocks, FILE *file) {

    // write blocks of compressed data to disk if need be
    if (blocks->size + block->size > INDEX_MEMORY_SIZE) {
        printf("\tBlocks in main memory full, writing blocks to file\n");
        pipe_to_file(blocks, file);
    }
    // add compressed block data to blocks
    memcpy(blocks->data + blocks->size, block->data, block->size);
    blocks->size += block->size;
    // clear out block
    block->size = 0;

    (*current_block_number)++;
}

// this function implements varbyte encoding, as recommended in lecture
size_t varbyte_encode(int value, unsigned char *output) {
    size_t i = 0;
    while (value >= 128) {
        output[i++] = (value & 127) | 128;
        value >>= 7;
    }
    output[i++] = value & 127;
    return i;
}

// this function takes a doc_id and count, compresses the doc_id,
// compresses the scor, and adds both to the appropriate blocks
void insert_posting(MemoryBlock *docids, MemoryBlock *freqs, int doc_id,
                    int count, int *current_block_number, MemoryBlock *blocks,
                    FILE *findex, LexiconEntry *current_entry) {

    size_t compressed_doc_size, compressed_freq_size;
    unsigned char compressed_doc_data[10];
    unsigned char compressed_freq_data[10];

    // compress doc_id and add to docids
    compressed_doc_size = varbyte_encode(doc_id, compressed_doc_data);

    // compress count and add to freqs
    compressed_freq_size = varbyte_encode(count, compressed_freq_data);

    if ((docids->size + compressed_doc_size) > BLOCK_SIZE) {
        // *** now that we aren't doing impact scores here anymore, should we also check for overflowing the frequencies block?
        // doc ids block is full, put docids block and freqs block in index
        // pad docids block with 0s so it is a full BLOCK_SIZE sized block
        if (docids->size < BLOCK_SIZE) {
            memset(docids->data + docids->size, 0,
                   BLOCK_SIZE - docids->size); // pad with 0s
            docids->size = BLOCK_SIZE;         // set size to BLOCK_SIZE
        }
        // pad frequency block with 0s so it is a full BLOCK_SIZE sized block
        if (freqs->size < BLOCK_SIZE) {
            memset(freqs->data + freqs->size, 0, BLOCK_SIZE - freqs->size);
            freqs->size = BLOCK_SIZE; // set size to BLOCK_SIZE
        }
        add_to_index(docids, current_block_number, blocks, findex);
        add_to_index(freqs, current_block_number, blocks, findex);
        current_entry->num_blocks++;
    }

    if (current_entry->start_d_block == -1) {
        // first posting of term is being inserted, set lexicon attributes correctly ***
        current_entry->start_d_block = *current_block_number;
        current_entry->start_d_offset = docids->size;
        current_entry->start_f_offset = freqs->size;
        current_entry->num_blocks = 0;
    }

    // add compressed docid and freq to docids and freqs blocks
    memcpy(docids->data + docids->size, compressed_doc_data,
           compressed_doc_size);
    docids->size += compressed_doc_size;
    // freqs->data[freqs->size++] = score;
    memcpy(freqs->data + freqs->size, compressed_freq_data,
           compressed_freq_size);
    freqs->size += compressed_freq_size;

    // Add the last inserted docID to the last array (updating)
    current_entry->last[current_entry->num_blocks] = doc_id;
}

// this is the main function that opens all of the files, scans in lines from
// the sorted postings list, and builds the inverted index
void create_inverted_index(const char *sorted_file_path) {
    // open sorted file
    FILE *fsorted_posts = fopen(sorted_file_path, "r");
    if (!fsorted_posts) {
        perror("Error opening sorted posts file");
        exit(EXIT_FAILURE);
    }

    // create index file
    FILE *findex = fopen("final_index.dat", "wb");
    if (!findex) {
        perror("Error opening final_index.dat");
        fclose(fsorted_posts);
        exit(EXIT_FAILURE);
    }

    FILE *flexi = fopen("lexicon_out", "wb");
    if (!flexi) {
        perror("Error opening lexicon_out");
        exit(EXIT_FAILURE);
    }

    // load_doc_lengths("docs_out.txt");
    read_words_out("words_out.txt");

    // Allocate memory for blocks array- this will hold all the compressed
    // blocks we can fill before piping to file
    MemoryBlock *blocks = malloc(sizeof(MemoryBlock));
    if (!blocks) {
        perror("Error allocating memory for blocks");
        exit(EXIT_FAILURE);
    }
    blocks->data = calloc(INDEX_MEMORY_SIZE, sizeof(unsigned char));
    if (!blocks->data) {
        perror("Error allocating memory for blocks->data");
        exit(EXIT_FAILURE);
    }
    blocks->size = 0;

    // block size buffer for docids
    MemoryBlock *docids = malloc(sizeof(MemoryBlock));
    if (!docids) {
        perror("Error allocating memory for docids");
        exit(EXIT_FAILURE);
    }
    docids->data = malloc(BLOCK_SIZE);
    if (!docids->data) {
        perror("Error allocating memory for docids->data");
        exit(EXIT_FAILURE);
    }
    docids->size = 0;

    // block size buffer for frequencies
    MemoryBlock *freqs = malloc(sizeof(MemoryBlock));
    if (!freqs) {
        perror("Error allocating memory for frequencies");
        exit(EXIT_FAILURE);
    }
    freqs->data = malloc(BLOCK_SIZE);
    if (!freqs->data) {
        perror("Error allocating memory for freqs->data");
        exit(EXIT_FAILURE);
    }
    freqs->size = 0;

    // initializing current block number, current term, word, and other
    // variables
    int current_block_number = 0;

    char *current_term =
        calloc(MAX_WORD_SIZE, sizeof(char)); // Zero out with calloc
    if (!current_term) {
        perror("Error allocating memory for current_term");
        exit(EXIT_FAILURE);
    }
    char *word = calloc(MAX_WORD_SIZE, sizeof(char)); // Zero out with calloc
    if (!word) {
        perror("Error allocating memory for word");
        exit(EXIT_FAILURE);
    }
    int count, doc_id;
    int last_doc_id = -1;
    int current_posting_did = -1;
    int current_posting_count = 0;

    LexiconEntry current_entry;
    memset(&current_entry, 0, sizeof(LexiconEntry));

    printf("Allocated memory for blocks, docids, and frequencies\n");
    printf("Starting to read sorted_posts.txt\n");

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &doc_id, &count) != EOF) {
        if (strcmp(current_term, word) != 0) {
            if (current_entry.term != NULL && current_entry.term[0] != '\0') {
                // encountering next term
                // insert current posting before moving onto next
                insert_posting(docids, freqs, current_posting_did,
                               current_posting_count, &current_block_number,
                               blocks, findex, &current_entry);
                current_entry.last_d_offset = docids->size;
                current_entry.last_f_offset = freqs->size;
                current_entry.last_d_block = current_block_number;
                current_entry.last_did = last_doc_id;

                // insert current entry into lexicon
                fprintf(
                    flexi, "%s %d %d %zu %zu %d %zu %zu %d %zu",
                    current_entry.term, current_entry.num_entries,
                    current_entry.start_d_block, current_entry.start_d_offset,
                    current_entry.start_f_offset, current_entry.last_d_block,
                    current_entry.last_d_offset, current_entry.last_f_offset,
                    current_entry.last_did, current_entry.num_blocks);
                for (size_t i = 0; i <= current_entry.num_blocks; i++) {
                    fprintf(flexi, " %d", current_entry.last[i]);
                }
                fprintf(flexi, "\n");

                // free current entry's allocated memory
                free(current_entry.term);
                free(current_entry
                         .last); // Free the memory allocated for the last array
            }

            // update current posting list's term
            strcpy(current_term, word);

            // update current posting's doc_id and count
            current_posting_did = doc_id;
            current_posting_count = 0;

            // Initialize current_entry for new term
            memset(
                &current_entry, 0,
                sizeof(LexiconEntry)); // Zero out the memory of current_entry
            current_entry.term = strdup(word);
            current_entry.start_d_block = -1;
            current_entry.start_d_offset = -1;
            current_entry.start_f_offset = -1;
            current_entry.count = 0;

            // Get the correct number of entries for the term so we can
            // calculate impact scores later
            TermEntry *term_entry;
            HASH_FIND_STR(terms, current_entry.term, term_entry);
            if (!term_entry) {
                fprintf(stderr, "Term not found in words_out.txt: %s\n",
                        current_entry.term);
                exit(EXIT_FAILURE);
            }
            current_entry.num_entries = term_entry->count;

            // Initialize the last array and num_blocks
            current_entry.last = malloc(sizeof(int) * MAX_BLOCKS);
            current_entry.num_blocks = 0;
        }

        // not a new term
        if (doc_id != current_posting_did) {
            // moved onto next posting in postings list
            insert_posting(docids, freqs, current_posting_did,
                           current_posting_count, &current_block_number, blocks,
                           findex, &current_entry);
            // Update current_posting_did and current_posting_count
            current_posting_did = doc_id;
            current_posting_count = count;
        } else {
            // same doc_id, still on current posting, so just update the count
            current_posting_count += count;
        }

        current_entry.count += count;
        last_doc_id = doc_id;
    }

    // add very last posting to docids and frequencies
    insert_posting(docids, freqs, current_posting_did, current_posting_count,
                   &current_block_number, blocks, findex, &current_entry);

    // fill in lexicon values for last term
    if (current_entry.term != NULL && current_entry.term[0] != '\0') {
        current_entry.last_d_offset = docids->size;
        current_entry.last_f_offset = freqs->size;
        current_entry.last_d_block = current_block_number;
        current_entry.last_did = last_doc_id;
        fprintf(flexi, "%s %d %d %zu %zu %d %zu %zu %d %zu", current_entry.term,
                current_entry.num_entries, current_entry.start_d_block,
                current_entry.start_d_offset, current_entry.start_f_offset,
                current_entry.last_d_block, current_entry.last_d_offset,
                current_entry.last_f_offset, current_entry.last_did,
                current_entry.num_blocks);
        for (size_t i = 0; i <= current_entry.num_blocks; i++) {
            fprintf(flexi, " %d", current_entry.last[i]);
        }
        fprintf(flexi, "\n");
        free(current_entry.term);
        free(current_entry.last);
    }

    // write the last blocks of docids and freqs to the blocks array
    if (freqs->size > 0 && docids->size > 0) {
        add_to_index(docids, &current_block_number, blocks, findex);
        add_to_index(freqs, &current_block_number, blocks, findex);
    }

    // Write remaining blocks array to file
    pipe_to_file(blocks, findex);

    // close files
    fclose(flexi);
    fclose(fsorted_posts);
    fclose(findex);

    // free buffers and blocks
    free(current_term);
    free(word);
    free(freqs->data);
    free(freqs);
    free(docids->data);
    free(docids);
    free(blocks->data);
    free(blocks);
}

int main(int argc, char *argv[]) {

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <sorted_file_path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    const char *sorted_file_path = argv[1];

    create_inverted_index(sorted_file_path);

    return 0;
}
