#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)190
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024) // 128MB
#define MAX_BLOCKS 1000 // Maximum number of blocks for one term- need to check this

typedef struct {
    size_t size;
    unsigned char *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char *term;
    int count;
    int start_d_block; // The block number where the first did resides
    size_t start_d_offset; // Offset within the block where the first docID is stored
    size_t start_f_offset; // Offset within the block where the first frequency is stored
    int last_d_block;  // the block number where the last did resides
    size_t last_d_offset; // Offset within the block where the last docID is stored
    size_t last_f_offset; // Offset within the block where the last frequency is stored
    int last_did;           // the last docID in the block
    int *last; // Array to store the last docID in each block
    size_t num_blocks; // Number of d blocks that the term's posting list spans
} LexiconEntry;


typedef struct {
    size_t size;
    unsigned char *data;
} CompressedData;

// this function opens the final index file, writes the blocks to the file,
// and resets the block size of each block
// *** added in some additional error handling
void pipe_to_file(CompressedData *blocks, FILE *file) {
    // Ensure the file is open
    if (!file) {
        perror("Error: File is not open");
        return;
    }

    // Write the data to the file
    size_t written = fwrite(blocks->data, 1, blocks->size, file);
    if (written != blocks->size) {
        perror("Error writing blocks to file");
        // Handle the error appropriately, e.g., by exiting or returning an error code
        exit(EXIT_FAILURE);
    }

    // Reset the blocks size
    blocks->size = 0;
}


void add_to_index(MemoryBlock *block, int *current_block_number,
                  CompressedData *blocks, FILE *file) {

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

void insert_posting(MemoryBlock *docids, MemoryBlock *frequencies, int doc_id, int count, 
                    int *current_block_number, CompressedData *blocks, FILE *findex, LexiconEntry *current_entry) {
    size_t compressed_doc_size;
    size_t compressed_freq_size;

    unsigned char compressed_doc_data[10];
    unsigned char compressed_freq_data[10];

    // compress doc_id and add to docids
    // printf("Compressing doc_id=%d\n", current_posting_did);
    compressed_doc_size = varbyte_encode(doc_id, compressed_doc_data);
    // printf("Compressing count=%d\n", current_posting_count);
    // compress count and add to frequencies
    compressed_freq_size = varbyte_encode(count, compressed_freq_data);

    // printf("Checking size of docs and freqs\n");
    if ((docids->size + compressed_doc_size > BLOCK_SIZE) || (frequencies->size + compressed_freq_size > BLOCK_SIZE)) {
        // printf("docids or freqs block is full, adding docids to index\n");
        
        add_to_index(docids, current_block_number, blocks, findex);
        // printf("now adding frequencies to index\n");
        add_to_index(frequencies, current_block_number, blocks, findex);
        current_entry->num_blocks++;
    }

    // printf("Adding compressed doc_id to docids\n");
    memcpy(docids->data + docids->size, compressed_doc_data, compressed_doc_size);
    docids->size += compressed_doc_size;

    // printf("Adding count to frequencies\n");
    memcpy(frequencies->data + frequencies->size, compressed_freq_data, compressed_freq_size);
    frequencies->size += compressed_freq_size;

    // Add the last inserted docID to the last array
    current_entry->last[current_entry->num_blocks] = doc_id;

}


void create_inverted_index(const char *sorted_file_path) {
    // open sorted file
    FILE *fsorted_posts = fopen(sorted_file_path, "r");
    if (!fsorted_posts) {
        perror("Error opening sorted_posts.txt");
        exit(EXIT_FAILURE);
    }
    else {
        printf("Opened sorted_posts.txt\n");
    }

    // create index file
    FILE *findex = fopen("final_index.dat", "wb");
    if (!findex) {
        perror("Error opening final_index.dat");
        fclose(fsorted_posts);
        exit(EXIT_FAILURE);
    }
    else {
        printf("Opened final_index.dat\n");
    }

    FILE *flexi = fopen("lexicon_out", "wb");
    if (!flexi) {
        perror("Error opening lexicon_out");
        exit(EXIT_FAILURE);
    }
    else {
        printf("Opened lexicon_out\n");
    }

    // Allocate memory for blocks array- this will hold all the compressed
    // blocks we can fill before piping to file
    CompressedData *blocks = malloc(sizeof(CompressedData));
    if (!blocks) {
        perror("Error allocating memory for blocks");
        exit(EXIT_FAILURE);
    }
    blocks->data = calloc(INDEX_MEMORY_SIZE, sizeof(unsigned char)); // Corrected allocation // *** changed to calloc to zero out so we don't have to pad with zeros if need be
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

    MemoryBlock *frequencies = malloc(sizeof(MemoryBlock));
    if (!frequencies) {
        perror("Error allocating memory for frequencies");
        exit(EXIT_FAILURE);
    }
    frequencies->data = malloc(BLOCK_SIZE); 
    if (!frequencies->data) {
        perror("Error allocating memory for frequencies->data");
        exit(EXIT_FAILURE);
    }
    frequencies->size = 0;

    int current_block_number = 0;

    char *current_term =
        calloc(MAX_WORD_SIZE, sizeof(char));          // Zero out with calloc
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


    // Initialize current_entry
    LexiconEntry current_entry;
    memset(&current_entry, 0, sizeof(LexiconEntry));

    printf("Allocated memory for blocks, docids, and frequencies\n");
    printf("Starting to read sorted_posts.txt\n");

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &doc_id, &count) != EOF) {
        // printf("\nScanning word: %s, count: %d, doc_id: %d\n", word, count, doc_id);
        if (strcmp(current_term, word) != 0) {
            // printf("Encountering new term- current term is %s, word is %s\n", current_term, word);
            if (current_entry.term != NULL && current_entry.term[0] != '\0') {
                // printf("Inserting last posting from last postings list\n");
                // encountering next term - need to write last posting in last posting list to dids and freqs
                insert_posting(docids, frequencies, current_posting_did, current_posting_count, &current_block_number, blocks, findex, &current_entry);
                // printf("last docid in last block: %d\n", current_entry.last[current_entry.num_blocks]);
                current_entry.last_d_offset = docids->size;
                current_entry.last_f_offset = frequencies->size;
                current_entry.last_d_block = current_block_number;
                current_entry.last_did = last_doc_id;

                // printf("Writing current_entry %s to lexicon_out\n", current_entry.term);
                fprintf(flexi, "%s %d %d %zu %zu %d %zu %zu %d", current_entry.term, current_entry.count,
                    current_entry.start_d_block, current_entry.start_d_offset, current_entry.start_f_offset,
                    current_entry.last_d_block, current_entry.last_d_offset, current_entry.last_f_offset,
                    current_entry.last_did);
                for (size_t i = 0; i <= current_entry.num_blocks; i++) {
                    fprintf(flexi, " %d", current_entry.last[i]);
                }
                fprintf(flexi, "\n");
                free(current_entry.term);
                free(current_entry.last); // Free the memory allocated for the last array
                // printf("Freed current_entry.term\n");
            }
            // update current posting list's term
            strcpy(current_term, word);

            // update current posting's doc_id and count 
            current_posting_did = doc_id;
            current_posting_count = 0;

            // Debug: Print the current term after update
            // printf("Current term updated to: '%s'\n", current_term);

            memset(&current_entry, 0, sizeof(LexiconEntry)); // Zero out the memory of current_entry
            current_entry.term = strdup(word); 
            current_entry.start_d_block = current_block_number;
            current_entry.start_d_offset = docids->size;
            current_entry.start_f_offset = frequencies->size;
            current_entry.count = 0; // Initialize count

            // Initialize the last array and num_blocks
            current_entry.last = malloc(sizeof(int) * MAX_BLOCKS); // MAX_BLOCKS is the maximum number of blocks
            current_entry.num_blocks = 0;

            // printf("Added new lexicon entry: term='%s', block_index=%d, offset=%zu, count=%d\n",
            //     current_entry.term, current_entry.block_index,
            //     current_entry.offset, current_entry.count);  
        }
        // not a new term
        if (doc_id != current_posting_did) {
            // moved onto next posting in postings list
            // printf("Moved onto next posting in postings list, add current posting to block\n");
            insert_posting(docids, frequencies, current_posting_did, current_posting_count, &current_block_number, blocks, findex, &current_entry);

            // Update current_posting_did and reset current_posting_count
            current_posting_did = doc_id;
            current_posting_count = count;
            // printf("Next posting is: %s in document %d\n", word, doc_id);
        }
        else {
            // same doc_id, still on current posting, so just update the count
            // printf("Still on same posting in document %d, add count %d to current posting count %d\n", doc_id, count, current_posting_count);
            current_posting_count += count;
            // printf("Still on same posting in document %d, posting count is now %d\n", doc_id, current_posting_count);
        }


        // add total word counts to lexicon and update last doc id, no matter whether new posting or not
        // printf("Adding %d to %d for lexicon count for term %s\n", count, current_entry.count, current_entry.term);
        current_entry.count += count;
        // printf("Total count for term %s = %d\n", current_entry.term, current_entry.count);
        last_doc_id = doc_id;
    }

    // add last posting to docids and frequencies
    insert_posting(docids, frequencies, current_posting_did, current_posting_count, &current_block_number, blocks, findex, &current_entry);
    
    printf("\nFinished scanning sorted_posts.txt\n");

    // finished - fill in lexicon for last value 
    if (current_entry.term != NULL && current_entry.term[0] != '\0') {
        current_entry.last_d_offset = docids->size;
        current_entry.last_f_offset = frequencies->size;
        current_entry.last_d_block = current_block_number;
        current_entry.last_did = last_doc_id; // Add last_doc_id to lexicon entry
        // printf("Writing final current_entry %s to lexicon_out\n", current_entry.term);
        fprintf(flexi, "%s %d %d %zu %zu %d %zu %zu %d", current_entry.term, current_entry.count,
            current_entry.start_d_block, current_entry.start_d_offset, current_entry.start_f_offset,
            current_entry.last_d_block, current_entry.last_d_offset, current_entry.last_f_offset,
            current_entry.last_did);
        for (size_t i = 0; i <= current_entry.num_blocks; i++) {
            fprintf(flexi, " %d", current_entry.last[i]);
        }
        fprintf(flexi, "\n");
        free(current_entry.term);
        free(current_entry.last); // Free the memory allocated for the last array
    }

    // write the last blocks to the index
    if (frequencies->size > 0 && docids->size > 0) {
        add_to_index(docids, &current_block_number, blocks, findex);
        add_to_index(frequencies, &current_block_number, blocks, findex);
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
    free(frequencies->data);
    free(frequencies);
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
