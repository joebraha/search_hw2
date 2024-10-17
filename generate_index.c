#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)190
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024) // 128MB

typedef struct {
    size_t size;
    unsigned char *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char *term;
    int count;
    size_t block_index;
    size_t offset;
    size_t last_did_block;  // the block number where the last did resides
    int last_did;           // the last docID in the block
    size_t last_did_offset; // Offset within the block where the last docID is
                            // stored
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

void create_inverted_index() {
    // open sorted file
    FILE *fsorted_posts = fopen("sorted_posts", "r");
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


    // Initial allocation for lexicon
    size_t lexicon_capacity =
        1300000; // a bit more than the real size (1224087)
    LexiconEntry *lexicon = malloc(lexicon_capacity * sizeof(LexiconEntry));
    if (!lexicon) {
        perror("Error allocating memory for lexicon");
        exit(EXIT_FAILURE);
    }
    size_t lexicon_size = 0;

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

    unsigned char compressed_data[10];

    printf("Allocated memory for blocks, docids, and frequencies\n");
    printf("Starting to read sorted_posts.txt\n");

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id) != EOF) {
        if (strcmp(current_term, word) != 0) {
            // encountering first term or next term-
            if (docids->size > 0 && frequencies->size > 0) {
                // Update last_did_offset for the current term before writing to
                // blocks
                lexicon[lexicon_size].last_did_offset = docids->size;
                lexicon[lexicon_size].last_did_block = current_block_number;
                lexicon[lexicon_size].last_did = last_doc_id; // Add last_doc_id to lexicon entry ***
                lexicon_size++;
            }

            // update current word
            strcpy(current_term, word);

            // Check if lexicon needs resizing
            if (lexicon_size >= lexicon_capacity) {
                lexicon_capacity *= 2;
                lexicon =
                    realloc(lexicon, lexicon_capacity * sizeof(LexiconEntry));
            }

            // add lexicon entry for new term
            lexicon[lexicon_size].term = malloc(MAX_WORD_SIZE);
            strcpy(lexicon[lexicon_size].term, word);
            lexicon[lexicon_size].block_index = current_block_number;
            lexicon[lexicon_size].offset = docids->size;
        }

        // moving check to before you add to the docids block, to prevent memory errors
        // if we tried to add a docid to an already full block, could result in seg fault ***
        // if (docids->size >= BLOCK_SIZE) {
        //     // 
        //     add_to_index(docids, &current_block_number, blocks, findex);
        //     add_to_index(frequencies, &current_block_number, blocks, findex);
        // }

        // not a new term, so add to existing postings list
        // compress doc_id and add to docids
        size_t compressed_size = varbyte_encode(doc_id, compressed_data);
        if (docids->size + compressed_size > BLOCK_SIZE) {
            printf("docids block is full, writing docids and frequencies to index\n");
            // doc ids block is full, write docids and freqs to index
            add_to_index(docids, &current_block_number, blocks, findex);
            add_to_index(frequencies, &current_block_number, blocks, findex);
        }
        memcpy(docids->data + docids->size, compressed_data, compressed_size);
        docids->size += compressed_size;

        // compress count and add to frequencies
        compressed_size = varbyte_encode(count, compressed_data);
        memcpy(frequencies->data + frequencies->size, compressed_data,
               compressed_size);
        // add total word counts to lexicon
        lexicon[lexicon_size].count += count;
    }

    // finished - fill in lexicon for last value 
    lexicon[lexicon_size].last_did_offset = docids->size;
    lexicon[lexicon_size].last_did_block = current_block_number;
    lexicon[lexicon_size].last_did = last_doc_id; // Add last_doc_id to lexicon entry
    lexicon_size++;

    // write the last blocks to the index
    if (frequencies->size > 0 && docids->size > 0) {
        add_to_index(docids, &current_block_number, blocks, findex);
        add_to_index(frequencies, &current_block_number, blocks, findex);
    }

    // Write remaining blocks to file
    pipe_to_file(blocks, findex);

    // pipe out lexicon
    FILE *flexi = fopen("lexicon_out", "wb");
    if (!flexi) {
        perror("Error opening lexicon_out");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < lexicon_size; i++) {
        LexiconEntry l = lexicon[i];
        // note: count is actually the frequency - 1 *** why?
        fprintf(flexi, "%s %d %zu %zu %zu %d %zu\n", l.term, l.count,
                l.block_index, l.offset, l.last_did_block, l.last_did, l.last_did_offset);
    }
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
    for (size_t i = 0; i < lexicon_size; i++) {
        free(lexicon[i].term);
    }
    free(lexicon);
}

int main() {

    // step 4: build inverted lists
    create_inverted_index();

    return 0;
}
