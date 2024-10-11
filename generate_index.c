#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE 65536 // 64KB

typedef struct {
    int doc_id;
    int length;
} DocEntry;

typedef struct {
    char *data;
    size_t size;
    size_t capacity;
} MemoryBlock;

typedef struct {
    char *term;
    size_t block_index;
    size_t offset;
} LexiconEntry;

MemoryBlock *create_memory_block(size_t capacity) {
    MemoryBlock *block = malloc(sizeof(MemoryBlock));
    block->data = malloc(capacity);
    block->size = 0;
    block->capacity = capacity;
    return block;
}

void free_memory_block(MemoryBlock *block) {
    free(block->data);
    free(block);
}

void add_to_memory_block(MemoryBlock *block, const void *data, size_t data_size) {
    if (block->size + data_size > block->capacity) {
        fprintf(stderr, "Error: Not enough space in the memory block\n");
        exit(EXIT_FAILURE);
    }
    memcpy(block->data + block->size, data, data_size);
    block->size += data_size;
}

void build_inverted_lists() {
    FILE *fsorted_posts = fopen("sorted_posts.txt", "r");
    if (!fsorted_posts) {
        perror("Error opening sorted_posts.txt");
        exit(EXIT_FAILURE);
    }

    MemoryBlock *current_block = create_memory_block(BLOCK_SIZE);
    MemoryBlock **blocks = malloc(sizeof(MemoryBlock *));
    size_t num_blocks = 1;
    blocks[0] = current_block;

    LexiconEntry *lexicon = NULL;
    size_t lexicon_size = 0;

    char current_term[256] = NULL;
    char word[256];
    int count, doc_id;
    int last_doc_id;

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id) != EOF) {
        if ((current_term == NULL) || strcmp(current_term, word) != 0) {
            // encountering first term or next term
            if (strcmp(current_term, word) != 0) {
                // new term, not first term
                // add last_doc_id to metadata array for block

                // postings list for current term is complete
                // compress list using var byte
                // if current block has enough space for full compressed list, add to current block
                // else, create new block and add compressed to old + current block (crosses block boundary)
                // make current block new block
            }

            // update current word
            strcpy(current_word, word);

            // add lexicon entry for new term 
            
            // initialize array of term's doc_ids and array frequencies

            // add doc_id and frequency to arrays
            
        }
        else {
            // not a new term, so add to existing postings list
            // add doc_id and frequency to arrays
            
        }

        

    }

    fclose(fsorted_posts);

}


int main() {
    // steps 1-3 are basically doing merge sort

    // step 1: split the file into smaller chunks
    system("split -l 1000000 testout_posts.txt chunk_");

    // step 2: sort each chunk
    system("for file in chunk_*; do sort -k1,1 \"$file\" -o \"$file.sorted\"; done");

    // step 3: merge sorted chunks
    system("sort -m -k1,1 chunk_*.sorted -o sorted_posts.txt");

    // step 4: build inverted lists
    build_inverted_lists();

    return 0;
}

// void read_doc_and_word_files() {
//     FILE *fdocs = fopen("testout_docs.txt", "r");
//     FILE *fwords = fopen("testout_words.txt", "r");

//     if (!fdocs || !fwords) {
//         perror("Error opening file");
//         exit(EXIT_FAILURE);
//     }

//     // read testout_docs.txt into doc_table, the array of DocEntry structs
//     while (!feof(fdocs)) {
//         doc_table = realloc(doc_table, (num_docs + 1) * sizeof(DocEntry));
//         fscanf(fdocs, "%d %d\n", &doc_table[num_docs].doc_id, &doc_table[num_docs].length);
//         num_docs++;
//     }
//     fclose(fdocs);

//     // read testout_words.txt into word_table, the array of WordEntry structs
//     while (!feof(fwords)) {
//         word_table = realloc(word_table, (num_words + 1) * sizeof(WordEntry));
//         word_table[num_words].word = malloc(256 * sizeof(char)); // Assuming max word length is 255
//         fscanf(fwords, "%s %d\n", word_table[num_words].word, &word_table[num_words].total_count);
//         num_words++;
//     }
//     fclose(fwords);
// }

// void build_inverted_lists() {
//     FILE *fsorted_posts = fopen("sorted_posts.txt", "r");
//     if (!fsorted_posts) {
//         perror("Error opening sorted_posts.txt");
//         exit(EXIT_FAILURE);
//     }

//     // initial allocation for inverted_lists
//     inverted_list_capacity = INITIAL_TERM_COUNT;
//     inverted_lists = malloc(inverted_list_capacity * sizeof(InvertedList));

//     char current_word[256];
//     InvertedList current_list;
//     current_list.word = NULL;
//     current_list.entries = NULL;
//     current_list.entry_count = 0;
//     current_list.entry_capacity = 0; // initialize capacity

//     while (!feof(fsorted_posts)) {
//         char word[256];
//         int count, doc_id;
//         fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id);

//         if (current_list.word == NULL || strcmp(current_list.word, word) != 0) {
//             // Save the current list if it exists
//             if (current_list.word != NULL) {
//                 if (num_inverted_lists >= inverted_list_capacity) {
//                     inverted_list_capacity *= 2;
//                     inverted_lists = realloc(inverted_lists, inverted_list_capacity * sizeof(InvertedList));
//                 }
//                 inverted_lists[num_inverted_lists] = current_list;
//                 num_inverted_lists++;
//             }

//             // start a new list
//             current_list.word = strdup(word);
//             current_list.entries = malloc(INITIAL_ENTRY_SIZE * sizeof(InvertedListEntry));
//             current_list.entry_count = 0;
//             current_list.entry_capacity = INITIAL_ENTRY_SIZE;
//         }

//         // add the posting to the current list
//         if (current_list.entry_count >= current_list.entry_capacity) {
//             current_list.entry_capacity *= 2;
//             current_list.entries = realloc(current_list.entries, current_list.entry_capacity * sizeof(InvertedListEntry));
//         }
//         current_list.entries[current_list.entry_count].doc_id = doc_id;
//         current_list.entries[current_list.entry_count].frequency = count;
//         current_list.entry_count++;
//     }

//     // save the last list
//     if (current_list.word != NULL) {
//         if (num_inverted_lists >= inverted_list_capacity) {
//             inverted_list_capacity *= 2;
//             inverted_lists = realloc(inverted_lists, inverted_list_capacity * sizeof(InvertedList));
//         }
//         inverted_lists[num_inverted_lists] = current_list;
//         num_inverted_lists++;
//     }

//     fclose(fsorted_posts);
// }

