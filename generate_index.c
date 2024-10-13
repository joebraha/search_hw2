#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)15
#define INDEX_MEMORY_SIZE (size_t)(128 * 1024 * 1024 / BLOCK_SIZE) // 128MB / 64KB


typedef struct {
    size_t size;
    size_t capacity;
    unsigned char *data; // Using unsigned char for byte-level operations
} MemoryBlock;

typedef struct {
    char term[MAX_WORD_SIZE];
    size_t block_index;
    size_t offset;
    size_t last_did_offset; // Offset within the block where the last docID is stored
} LexiconEntry;


// this function opens the final index file, writes the blocks to the file, and resets the block size of each block 
void pipe_to_file(MemoryBlock *blocks, size_t num_blocks, FILE *file) {
    for (size_t i = 0; i < num_blocks; i++) {
        fwrite(blocks[i].data, 1, blocks[i].size, file);
        blocks[i].size = 0; // Reset block size after writing to file
    }
}


// v2 - this function takes in a buffer containing compressed frequency or docid data,
// the size of the buffer, the offset in the block to start writing to, the current 
// block number, and the array of MemoryBlocks. It writes the buffer to the blocks array, 
// updating the offset and block number as needed.
size_t add_to_index(unsigned char *buffer, size_t buffer_size, size_t offset, int *current_block_number, MemoryBlock *blocks, FILE *file) {
    while (buffer_size > 0) {
        // still data left to write, keep copying over
        size_t space_left = BLOCK_SIZE - offset;
        if (buffer_size <= space_left) {
            // space available in current block for whole data
            memcpy(blocks[*current_block_number].data + offset, buffer, buffer_size);
            offset += buffer_size;
            buffer_size = 0;
        } else {
            // data is larger than space available in current block, add what we can and move to next block
            memcpy(blocks[*current_block_number].data + offset, buffer, space_left);
            buffer += space_left;
            buffer_size -= space_left;
            (*current_block_number)++;
            offset = 0;
            if (*current_block_number >= INDEX_MEMORY_SIZE) {
                pipe_to_file(blocks, INDEX_MEMORY_SIZE, "final_index.dat");
                *current_block_number = 0;
            }
        }
    }
    return offset;
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
    FILE *fsorted_posts = fopen("sorted_posts.txt", "r");
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

   // Allocate memory for blocks array- this will hold all the blocks we can fill before piping to file
    MemoryBlock *blocks = malloc(INDEX_MEMORY_SIZE * sizeof(MemoryBlock));
    for (size_t i = 0; i < INDEX_MEMORY_SIZE; i++) {
        blocks[i].size = 0;
        blocks[i].capacity = BLOCK_SIZE;
        blocks[i].data = malloc(BLOCK_SIZE);
    }
    size_t blocks_offset = 0;

    // Temporary buffers for docids and frequencies
    // keep track of their sizes and capacities so we can reallocate more space as necessary
    size_t docids_capacity = 1024;
    size_t frequencies_capacity = 1024;
    unsigned char *docids = malloc(docids_capacity);
    unsigned char *frequencies = malloc(frequencies_capacity);
    size_t docids_size = 0;
    size_t frequencies_size = 0;

    int current_block_number = 0;

    // Initial allocation for lexicon
    size_t lexicon_capacity = 1000; // Initial size of lexicon- real size more like 1 mil probably? need to check but can't bc current word list contains duplicates
    LexiconEntry *lexicon = malloc(lexicon_capacity * sizeof(LexiconEntry));
    size_t lexicon_size = 0;

    char *current_term = calloc(MAX_WORD_SIZE, sizeof(char)); // Zero out with calloc
    char *word = calloc(MAX_WORD_SIZE, sizeof(char)); // Zero out with calloc
    int count, doc_id;
    int last_doc_id = -1;

    unsigned char compressed_data[10]; // Buffer for compressed data

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id) != EOF) { 
        if (strcmp(current_term, word) != 0) {
            // encountering first term or next term- need to write the current term's data to blocks
            if (frequencies_size > 0 && docids_size > 0) {
                // Update last_did_offset for the current term before writing to blocks
                lexicon[lexicon_size - 1].last_did_offset = blocks_offset + docids_size;

                blocks_offset = add_to_index(docids, docids_size, blocks_offset, &current_block_number, blocks, findex);
                blocks_offset = add_to_index(frequencies, frequencies_size, blocks_offset, &current_block_number, blocks, findex);
                docids_size = 0;
                frequencies_size = 0;
            }

            // update current word
            strcpy(current_term, word);

            // Check if lexicon needs resizing
            if (lexicon_size >= lexicon_capacity) {
                lexicon_capacity *= 2;
                lexicon = realloc(lexicon, lexicon_capacity * sizeof(LexiconEntry));
            }

            // add lexicon entry for new term
            lexicon[lexicon_size].term = strdup(word);
            lexicon[lexicon_size].block_index = current_block_number;
            lexicon[lexicon_size].offset = blocks_offset;
            lexicon[lexicon_size].last_did_offset = blocks_offset + docids_size; // Initialize last_did_offset
            lexicon_size++;
        }

        // not a new term, so add to existing postings list
        // compress count
        size_t compressed_size = varbyte_encode(count, compressed_data);
        // check if we need to reallocate memory for frequencies buffer
        if (frequencies_size + compressed_size > frequencies_capacity) {
            frequencies_capacity *= 2;
            frequencies = realloc(frequencies, frequencies_capacity);
        }
        // add compressed count to frequencies buffer
        memcpy(frequencies + frequencies_size, compressed_data, compressed_size);
        frequencies_size += compressed_size;

        // compress doc_id
        compressed_size = varbyte_encode(doc_id, compressed_data);
        // check if we need to reallocate memory for docids buffer
        if (docids_size + compressed_size > docids_capacity) {
            docids_capacity *= 2;
            docids = realloc(docids, docids_capacity);
        }
        // add compressed doc_id to docids buffer
        memcpy(docids + docids_size, compressed_data, compressed_size);
        docids_size += compressed_size;

    }

    // check if there is any data left to write from the last line in the file
    if (frequencies_size > 0 && docids_size > 0) {
        blocks_offset = add_to_index(docids, docids_size, blocks_offset, &current_block_number, blocks, findex);
        blocks_offset = add_to_index(frequencies, frequencies_size, blocks_offset, &current_block_number, blocks, findex);
    }


    // Write remaining blocks to file
    pipe_to_file(blocks, INDEX_MEMORY_SIZE, findex);

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
    // steps 1-3 doing merge sort

    // step 1: split the file into smaller chunks
    system("split -l 1000000 testout_posts.txt chunk_");

    // step 2: sort each chunk
    system("for file in chunk_*; do sort -k1,1 \"$file\" -o \"$file.sorted\"; "
           "done");

    // step 3: merge sorted chunks
    system("sort -m -k1,1 chunk_*.sorted -o sorted_posts.txt");

    // step 4: build inverted lists
    create_inverted_index();

    return 0;
}


// QUESTIONS
// what does the doc table actually do? currently not implemented
// what does the word table do here that isn't currently done by the creation of the lexicon?
// do we need to keep track of the total frequency of a word in the lexicon?
// how did we decide on 128MB for the index memory size?
// should we do the merge sort before we even run this whole program just to see if it works?

// POTENTIAL OPTIMIZATIONS
// initial capacities for buffers and the lexicon are arbitrary, consider using more adaptive strategies for resizing? how to pick?
// add more error checking for malloc and realloc calls
// pass in file names as arguments to the program
// PIPE OUT LEXICON TO A SEPARATE FILE !!! NOT IMPLEMENTED YET
// think about doing merge sort outside of the program
// abstract out parts of massive build index function so it's more readable and less of an eyesore