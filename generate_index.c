#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)190
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024) // 128MB

typedef struct {
    size_t size;
    int *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char *term;
    int count;
    size_t block_index;
    size_t offset;
    size_t last_did_block;  // the block number where the last did resides
    size_t last_did_offset; // Offset within the block where the last docID is
                            // stored
} LexiconEntry;

typedef struct {
    size_t size;
    int *data;
} CompressedData;

// this function opens the final index file, writes the blocks to the file,
// and resets the block size of each block
void pipe_to_file(CompressedData *blocks, FILE *file) {
    printf("pipe_to_file\n");
    fwrite(blocks->data, 1, blocks->size, file);
    blocks->size = 0;
}

// TODO: this
CompressedData *compress_block(MemoryBlock *block) {
    CompressedData *c = malloc(sizeof(CompressedData));
    c->size = block->size;
    c->data = block->data; // note that this rn doesn't copy the data
    return c;
}

// v2 - this function takes in a buffer containing compressed frequency or docid
// data, the size of the buffer, the offset in the block to start writing to,
// the current block number, and the array of MemoryBlocks. It writes the buffer
// to the blocks array, updating the offset and block number as needed.
void add_to_index(MemoryBlock *block, int *current_block_number,
                  CompressedData *blocks, FILE *file) {
    printf("add_to_index, block: %d\n", *current_block_number);
    // compress block
    CompressedData *compressed = compress_block(block);
    // write buffer of compressed data to disk if need be
    if (blocks->size + compressed->size > INDEX_MEMORY_SIZE / sizeof(int)) {
        pipe_to_file(blocks, file);
    }
    // add to buffer of compressed data
    memcpy(blocks->data + blocks->size, compressed->data, compressed->size);
    blocks->size += compressed->size;
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

    // create index file
    FILE *findex = fopen("final_index.dat", "wb");
    if (!findex) {
        perror("Error opening final_index.dat");
        fclose(fsorted_posts);
        exit(EXIT_FAILURE);
    }

    // Allocate memory for blocks array- this will hold all the compressed
    // blocks we can fill before piping to file
    CompressedData *blocks = malloc(sizeof(CompressedData));
    blocks->data = malloc(INDEX_MEMORY_SIZE);

    // Temporary buffers for docids and frequencies
    // keep track of their sizes and capacities so we can reallocate more space
    // as necessary
    MemoryBlock *docids = malloc(sizeof(MemoryBlock));
    docids->data = malloc(BLOCK_SIZE);
    MemoryBlock *frequencies = malloc(sizeof(MemoryBlock));
    frequencies->data = malloc(BLOCK_SIZE);

    int current_block_number = 0;

    // Initial allocation for lexicon
    size_t lexicon_capacity =
        1300000; // a bit more than the real size (1224087)
    LexiconEntry *lexicon = malloc(lexicon_capacity * sizeof(LexiconEntry));
    size_t lexicon_size = 0;

    char *current_term =
        calloc(MAX_WORD_SIZE, sizeof(char));          // Zero out with calloc
    char *word = calloc(MAX_WORD_SIZE, sizeof(char)); // Zero out with calloc
    int count, doc_id;
    int last_doc_id = -1;

    unsigned char compressed_data[10]; // Buffer for compressed data

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id) != EOF) {
        // printf("here\n");
        printf("%s %d %d\n", word, doc_id, count);
        if (strcmp(current_term, word) != 0) {
            // encountering first term or next term- need to write the current
            // term's data to blocks
            if (docids->size > 0 && frequencies->size > 0) {
                // Update last_did_offset for the current term before writing to
                // blocks
                lexicon[lexicon_size].last_did_offset = docids->size;
                lexicon[lexicon_size].last_did_block = current_block_number;
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

        if (docids->size >= BLOCK_SIZE) {
            // compress and write to block
            add_to_index(docids, &current_block_number, blocks, findex);
            add_to_index(frequencies, &current_block_number, blocks, findex);
        }

        // not a new term, so add to existing postings list
        docids->data[docids->size++] = doc_id;
        frequencies->data[frequencies->size++] = count;
        // add total word counts to lexicon
        lexicon[lexicon_size].count += count;

        // // compress count
        // size_t compressed_size = varbyte_encode(count, compressed_data);
        // // check if we need to reallocate memory for frequencies buffer
        // if (frequencies_size + compressed_size > frequencies_capacity) {
        //     frequencies_capacity *= 2;
        //     frequencies = realloc(frequencies, frequencies_capacity);
        // }
        // // add compressed count to frequencies buffer
        // memcpy(frequencies + frequencies_size, compressed_data,
        //        compressed_size);
        // frequencies_size += compressed_size;
        //
        // // compress doc_id
        // compressed_size = varbyte_encode(doc_id, compressed_data);
        // // check if we need to reallocate memory for docids buffer
        // if (docids_size + compressed_size > docids_capacity) {
        //     docids_capacity *= 2;
        //     docids = realloc(docids, docids_capacity);
        // }
        // // add compressed doc_id to docids buffer
        // memcpy(docids + docids_size, compressed_data, compressed_size);
        // docids_size += compressed_size;
    }

    // finished - fill in lexicon for last value
    lexicon[lexicon_size].last_did_offset = docids->size;
    lexicon[lexicon_size].last_did_block = current_block_number;
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
        // note: count is actually the frequency - 1
        fprintf(flexi, "%s %d %zu %zu %zu %zu\n", l.term, l.count,
                l.block_index, l.offset, l.last_did_block, l.last_did_offset);
    }
    fclose(flexi);

    // close files
    fclose(fsorted_posts);
    fclose(findex);

    // free buffers and blocks
    free(current_term);
    free(word);
    free(frequencies);
    free(docids);
    for (size_t i = 0; i < INDEX_MEMORY_SIZE; i++) {
        free(blocks[i].data);
    }
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

// QUESTIONS
// what does the doc table actually do? currently not implemented
// what does the word table do here that isn't currently done by the creation of
// the lexicon? do we need to keep track of the total frequency of a word in the
// lexicon? how did we decide on 128MB for the index memory size? should we do
// the merge sort before we even run this whole program just to see if it works?

// POTENTIAL OPTIMIZATIONS
// initial capacities for buffers and the lexicon are arbitrary, consider using
// more adaptive strategies for resizing? how to pick? add more error checking
// for malloc and realloc calls pass in file names as arguments to the program
// PIPE OUT LEXICON TO A SEPARATE FILE !!! NOT IMPLEMENTED YET
// think about doing merge sort outside of the program
// abstract out parts of massive build index function so it's more readable and
// less of an eyesore
