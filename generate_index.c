#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)190
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024) // 128MB
#define MAX_BLOCKS                                                             \
    1000 // Maximum number of blocks for one term- need to check this
#define N_DOCUMENTS                                                            \
    8841823 // Number of documents in the collection *** how many docs in
            // collection?

typedef struct {
    size_t size;
    unsigned char *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char *term;
    int count;
    int num_entries; // the number of entries for this word. In other words, the
                     // number of documents containing this word
    int start_d_block;     // The block number where the first did resides
    size_t start_d_offset; // Offset within the block where the first docID is
                           // stored
    size_t start_f_offset; // Offset within the block where the first frequency
                           // is stored
    int last_d_block;      // the block number where the last did resides
    size_t
        last_d_offset; // Offset within the block where the last docID is stored
    size_t last_f_offset; // Offset within the block where the last frequency is
                          // stored
    int last_did;         // the last docID in the block
    int *last;            // Array to store the last docID in each block
    size_t num_blocks; // Number of d blocks that the term's posting list spans
} LexiconEntry;

typedef struct {
    size_t size;
    unsigned char *data;
} CompressedData;

// Define the array for the docs table
int *doc_table = NULL;

// this function opens the final index file, writes the blocks to the file,
// and resets the block size of each block
// *** added in some additional error handling
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
        // Handle the error appropriately, e.g., by exiting or returning an
        // error code
        exit(EXIT_FAILURE);
    }

    // Reset the blocks size
    blocks->size = 0;
}

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
        // printf("%d, %d\n", doc_id, doc_length);
        doc_table[doc_id] = doc_length;
    }
    // printf("%d\n", doc_table[6000]);
    // exit(1);

    fclose(file);
}

// function to calculate BM25 score of a single word in a document
double get_score(int freq, int doc_id, int num_entries) {
    double k1 = 1.2;               // free parameter
    double b = 0.75;               // free parameter
    double avg_doc_length = 66.93; // average document length
    int d = doc_table[doc_id];     // length of this document

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

    // usually between 1 and 2, so scale by 100 and store the integer value in a
    // byte
    unsigned char ret = (unsigned char)(score * 100);
    // printf("%d, %u, %f\n", num_entries, ret, score);
    return ret;
}

void add_to_index(MemoryBlock *block, int *current_block_number,
                  MemoryBlock *blocks, FILE *file) {

    // *** the block coming in now should already be compressed
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

void insert_posting(MemoryBlock *docids, MemoryBlock *scores, int doc_id,
                    int count, int *current_block_number, MemoryBlock *blocks,
                    FILE *findex, LexiconEntry *current_entry) {
    size_t compressed_doc_size;

    unsigned char compressed_doc_data[10];

    // compress doc_id and add to docids
    // printf("\tCompressing doc_id=%d\n", doc_id);
    compressed_doc_size = varbyte_encode(doc_id, compressed_doc_data);
    // printf("\tCompressing count=%d\n", count);
    // compress count and add to frequencies
    unsigned char score = get_score(count, doc_id, current_entry->num_entries);

    // printf("Checking size of docs and freqs\n");
    if (docids->size + compressed_doc_size > BLOCK_SIZE) {
        // printf("\tdocids or freqs block is full, adding docids to index\n");

        // pad frequency block with 0s
        if (scores->size < BLOCK_SIZE) {
            memset(scores->data + scores->size, 0, BLOCK_SIZE - scores->size);
        }
        add_to_index(docids, current_block_number, blocks, findex);
        // printf("now adding frequencies to index\n");
        add_to_index(scores, current_block_number, blocks, findex);
        current_entry->num_blocks++;
    }

    // printf("\tAdding compressed doc_id to docids\n");
    memcpy(docids->data + docids->size, compressed_doc_data,
           compressed_doc_size);
    docids->size += compressed_doc_size;

    // printf("\tAdding score to frequencies\n");
    scores->data[scores->size++] = score;

    // Add the last inserted docID to the last array
    current_entry->last[current_entry->num_blocks] = doc_id;
}

void create_inverted_index(const char *sorted_file_path) {
    // open sorted file
    FILE *fsorted_posts = fopen(sorted_file_path, "r");
    if (!fsorted_posts) {
        perror("Error opening sorted_posts.txt");
        exit(EXIT_FAILURE);
    } else {
        printf("Opened sorted_posts.txt\n");
    }

    // create index file
    FILE *findex = fopen("final_index.dat", "wb");
    if (!findex) {
        perror("Error opening final_index.dat");
        fclose(fsorted_posts);
        exit(EXIT_FAILURE);
    } else {
        printf("Opened final_index.dat\n");
    }

    FILE *flexi = fopen("lexicon_out", "wb");
    if (!flexi) {
        perror("Error opening lexicon_out");
        exit(EXIT_FAILURE);
    } else {
        printf("Opened lexicon_out\n");
    }

    load_doc_lengths("parser/testout_docs.txt");

    // Allocate memory for blocks array- this will hold all the compressed
    // blocks we can fill before piping to file
    MemoryBlock *blocks = malloc(sizeof(MemoryBlock));
    if (!blocks) {
        perror("Error allocating memory for blocks");
        exit(EXIT_FAILURE);
    }
    blocks->data =
        calloc(INDEX_MEMORY_SIZE,
               sizeof(unsigned char)); // Corrected allocation // *** changed to
                                       // calloc to zero out so we don't have to
                                       // pad with zeros if need be
    if (!blocks->data) {
        perror("Error allocating memory for blocks->data");
        exit(EXIT_FAILURE);
    }
    blocks->size = 0;

    // Temporary buffers for docids and frequencies to go into one block
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

    MemoryBlock *scores = malloc(sizeof(MemoryBlock));
    if (!scores) {
        perror("Error allocating memory for frequencies");
        exit(EXIT_FAILURE);
    }
    scores->data = malloc(BLOCK_SIZE);
    if (!scores->data) {
        perror("Error allocating memory for frequencies->data");
        exit(EXIT_FAILURE);
    }
    scores->size = 0;

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

    // debug
    // int num_postings_inserted = 0;

    // Initialize current_entry
    LexiconEntry current_entry;
    memset(&current_entry, 0, sizeof(LexiconEntry));

    printf("Allocated memory for blocks, docids, and frequencies\n");
    printf("Starting to read sorted_posts.txt\n");

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &doc_id, &count) != EOF) {
        // printf("\nScanning word: %s, count: %d, doc_id: %d\n", word, count,
        // doc_id);
        if (strcmp(current_term, word) != 0) {
            // printf("\tEncountering new term- current term is %s, word is
            // %s\n", current_term, word);
            if (current_entry.term != NULL && current_entry.term[0] != '\0') {
                // encountering next term - need to write last posting in last
                // posting list to dids and freqs printf("\tInserting last
                // posting from last postings list\n"); num_postings_inserted++;
                // printf("\t%d postings inserted\n", num_postings_inserted);
                insert_posting(docids, scores, current_posting_did,
                               current_posting_count, &current_block_number,
                               blocks, findex, &current_entry);
                // printf("last docid in last block: %d\n",
                // current_entry.last[current_entry.num_blocks]);
                current_entry.last_d_offset = docids->size;
                current_entry.last_f_offset = scores->size;
                current_entry.last_d_block = current_block_number;
                current_entry.last_did = last_doc_id;

                // printf("\tWriting current_entry %s to lexicon_out\n",
                // current_entry.term);
                fprintf(flexi, "%s %d %d %d %zu %zu %d %zu %zu %d %zu",
                        current_entry.term, current_entry.count,
                        current_entry.num_entries, current_entry.start_d_block,
                        current_entry.start_d_offset,
                        current_entry.start_f_offset,
                        current_entry.last_d_block, current_entry.last_d_offset,
                        current_entry.last_f_offset, current_entry.last_did,
                        current_entry.num_blocks);
                for (size_t i = 0; i <= current_entry.num_blocks; i++) {
                    fprintf(flexi, " %d", current_entry.last[i]);
                }
                fprintf(flexi, "\n");
                free(current_entry.term);
                free(current_entry
                         .last); // Free the memory allocated for the last array
                // printf("Freed current_entry.term\n");
            }
            // update current posting list's term
            strcpy(current_term, word);

            // update current posting's doc_id and count
            current_posting_did = doc_id;
            current_posting_count = 0;

            // Debug: Print the current term after update
            // printf("\tCurrent term updated to: '%s'\n", current_term);

            memset(
                &current_entry, 0,
                sizeof(LexiconEntry)); // Zero out the memory of current_entry
            current_entry.term = strdup(word);
            current_entry.start_d_block = current_block_number;
            current_entry.start_d_offset = docids->size;
            current_entry.start_f_offset = scores->size;
            current_entry.count = 0; // Initialize count
            current_entry.num_entries = 1;

            // Initialize the last array and num_blocks
            current_entry.last = malloc(
                sizeof(int) *
                MAX_BLOCKS); // MAX_BLOCKS is the maximum number of blocks
            current_entry.num_blocks = 0;

            // printf("\tAdded new lexicon entry: term='%s', start_d_block=%d,
            // start_d_offset=%zu, start_f_offset=%zu, count=%d\n",
            //     current_entry.term, current_entry.start_d_block,
            //     current_entry.start_d_offset, current_entry.start_f_offset,
            //     current_entry.count);
        }
        // not a new term
        if (doc_id != current_posting_did) {
            // moved onto next posting in postings list
            // printf("\tMoved onto next posting in postings list, add current
            // posting to block\n");
            insert_posting(docids, scores, current_posting_did,
                           current_posting_count, &current_block_number, blocks,
                           findex, &current_entry);
            // printf("\tInserted %d postings\n", num_postings_inserted);
            // num_postings_inserted++;
            // Update current_posting_did and reset current_posting_count
            current_posting_did = doc_id;
            current_posting_count = count;
            current_entry.num_entries++;
            // printf("\tNext posting is: %s in document %d\n", word, doc_id);
        } else {
            // same doc_id, still on current posting, so just update the count
            // printf("\tStill on same posting in document %d, add count %d to
            // current posting count %d\n", doc_id, count,
            // current_posting_count);
            current_posting_count += count;
            // printf("\tStill on same posting in document %d, posting count is
            // now %d\n", doc_id, current_posting_count);
        }

        // add total word counts to lexicon and update last doc id, no matter
        // whether new posting or not printf("Adding %d to %d for lexicon count
        // for term %s\n", count, current_entry.count, current_entry.term);
        current_entry.count += count;
        // printf("Total count for term %s = %d\n", current_entry.term,
        // current_entry.count);
        last_doc_id = doc_id;
    }

    // add last posting to docids and frequencies
    // printf("\nInserting last posting from last postings list\n");
    insert_posting(docids, scores, current_posting_did, current_posting_count,
                   &current_block_number, blocks, findex, &current_entry);

    // printf("\nFinished scanning sorted_posts.txt\n");

    // finished - fill in lexicon for last value
    if (current_entry.term != NULL && current_entry.term[0] != '\0') {
        current_entry.last_d_offset = docids->size;
        current_entry.last_f_offset = scores->size;
        current_entry.last_d_block = current_block_number;
        current_entry.last_did =
            last_doc_id; // Add last_doc_id to lexicon entry
        // printf("Writing final current_entry %s to lexicon_out\n",
        // current_entry.term);
        fprintf(flexi, "%s %d %d %d %zu %zu %d %zu %zu %d %zu",
                current_entry.term, current_entry.count,
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
        free(
            current_entry.last); // Free the memory allocated for the last array
    }

    // write the last blocks to the index
    if (scores->size > 0 && docids->size > 0) {
        add_to_index(docids, &current_block_number, blocks, findex);
        add_to_index(scores, &current_block_number, blocks, findex);
    }

    // Write remaining blocks to file
    pipe_to_file(blocks, findex);

    fclose(flexi);

    // close files
    fclose(fsorted_posts);
    fclose(findex);

    // free buffers and blocks
    free(current_term);
    free(word);
    free(scores->data);
    free(scores);
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

    // step 4: build inverted lists
    create_inverted_index(sorted_file_path);

    return 0;
}
