#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_WORD_SIZE (size_t)15
#define INDEX_MEMORY_SIZE                                                      \
    ((size_t)128 << 10) /                                                      \
        BLOCK_SIZE // 128 MB / block size should give the number of active
                   // blocks at a time (if I did my math right)

typedef struct { // do we even need this?
    int doc_id;
    int length;
} DocEntry;

typedef struct {
    size_t size;
    size_t capacity;
    int data[BLOCK_SIZE / sizeof(int) -
             2 * sizeof(size_t)]; // in-place array of int values
} MemoryBlock;

typedef struct {
    char *term;
    size_t block_index;
    size_t offset;
    size_t last_did_block;
    size_t last_did_osset;
} LexiconEntry;

// adds the given integer to the block passed in. Used to populate frequency
// list or docid list
void add_to_memory_block(MemoryBlock *block, int data) {
    if (block->size + sizeof(int) > block->capacity) {
        fprintf(stderr, "Error: Not enough space in the memory block\n");
        exit(EXIT_FAILURE);
    }
    memcpy(&block->data + block->size, &data, sizeof(int));
    block->size += sizeof(int);
}

void pipe_to_file() {
    // when memory use gets too big, we'l want to compress and append the list
    // data to file, and clear the allocated space
}

size_t add_to_index(MemoryBlock *block, size_t offset,
                    int *current_block_number) {
    // add the block passed in to the index, return new offset for index

    if (offset >= INDEX_MEMORY_SIZE) {
        pipe_to_file();
        offset = 0;
    }

    // do the copy

    (*current_block_number)++;
    return offset + 1;
}

void build_inverted_lists() {
    FILE *fsorted_posts = fopen("sorted_posts.txt", "r");
    if (!fsorted_posts) {
        perror("Error opening sorted_posts.txt");
        exit(EXIT_FAILURE);
    }

    // holds the big memory buffer of concatenated blocks
    MemoryBlock *blocks = malloc(BLOCK_SIZE * INDEX_MEMORY_SIZE);
    // holds a pointer to the next position in the big buffer to insert new
    // blocks
    size_t blocks_offset = 0;

    // these hold the in-progress blocks of dids and freqs. When they fill,
    // add them to the blocks array.
    MemoryBlock *frequencies = malloc(BLOCK_SIZE);
    MemoryBlock *docids = malloc(BLOCK_SIZE);
    int current_block_number = 0;

    LexiconEntry *lexicon = NULL; // todo: malloc
    size_t lexicon_size = 0;

    char *current_term = malloc(MAX_WORD_SIZE); // todo: zero these out
    char *word = malloc(MAX_WORD_SIZE);
    int count, doc_id;
    int last_doc_id;

    while (fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id) !=
           EOF) { // TODO: change to fgets to match file format
        if ((current_term == NULL) || strcmp(current_term, word) != 0) {
            // encountering first term or next term
            if (strcmp(current_term, word) != 0) {
                // new term, not first term
                // add last_doc_id to lexicon

                lexicon[lexicon_size - 1].last_did_block = current_block_number;
                lexicon[lexicon_size - 1].last_did_osset = docids->size - 1;
            }

            // update current word
            strcpy(current_term, word);

            // add lexicon entry for new term
            LexiconEntry entry = lexicon[lexicon_size];
            lexicon_size++;
            entry.term = malloc(MAX_WORD_SIZE);
            strcpy(entry.term, word);
            entry.offset = docids->size;
            entry.block_index = current_block_number;
        }
        // not a new term, so add to existing postings list
        // add doc_id and frequency to arrays
        if (frequencies->size + sizeof(int) > frequencies->capacity) {
            // move blocks to index and clear their memory
            blocks_offset =
                add_to_index(docids, blocks_offset, &current_block_number);
            blocks_offset =
                add_to_index(frequencies, blocks_offset, &current_block_number);
        }

        // now everything should be in order to just add the values to the
        // blocks
        add_to_memory_block(frequencies, count);
        add_to_memory_block(docids, doc_id);
    }

    fclose(fsorted_posts);
}

int main() {
    // steps 1-3 are basically doing merge sort

    // step 1: split the file into smaller chunks
    system("split -l 1000000 testout_posts.txt chunk_");

    // step 2: sort each chunk
    system("for file in chunk_*; do sort -k1,1 \"$file\" -o \"$file.sorted\"; "
           "done");

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
//         fscanf(fdocs, "%d %d\n", &doc_table[num_docs].doc_id,
//         &doc_table[num_docs].length); num_docs++;
//     }
//     fclose(fdocs);

//     // read testout_words.txt into word_table, the array of WordEntry structs
//     while (!feof(fwords)) {
//         word_table = realloc(word_table, (num_words + 1) *
//         sizeof(WordEntry)); word_table[num_words].word = malloc(256 *
//         sizeof(char)); // Assuming max word length is 255 fscanf(fwords, "%s
//         %d\n", word_table[num_words].word,
//         &word_table[num_words].total_count); num_words++;
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

//         if (current_list.word == NULL || strcmp(current_list.word, word) !=
//         0) {
//             // Save the current list if it exists
//             if (current_list.word != NULL) {
//                 if (num_inverted_lists >= inverted_list_capacity) {
//                     inverted_list_capacity *= 2;
//                     inverted_lists = realloc(inverted_lists,
//                     inverted_list_capacity * sizeof(InvertedList));
//                 }
//                 inverted_lists[num_inverted_lists] = current_list;
//                 num_inverted_lists++;
//             }

//             // start a new list
//             current_list.word = strdup(word);
//             current_list.entries = malloc(INITIAL_ENTRY_SIZE *
//             sizeof(InvertedListEntry)); current_list.entry_count = 0;
//             current_list.entry_capacity = INITIAL_ENTRY_SIZE;
//         }

//         // add the posting to the current list
//         if (current_list.entry_count >= current_list.entry_capacity) {
//             current_list.entry_capacity *= 2;
//             current_list.entries = realloc(current_list.entries,
//             current_list.entry_capacity * sizeof(InvertedListEntry));
//         }
//         current_list.entries[current_list.entry_count].doc_id = doc_id;
//         current_list.entries[current_list.entry_count].frequency = count;
//         current_list.entry_count++;
//     }

//     // save the last list
//     if (current_list.word != NULL) {
//         if (num_inverted_lists >= inverted_list_capacity) {
//             inverted_list_capacity *= 2;
//             inverted_lists = realloc(inverted_lists, inverted_list_capacity *
//             sizeof(InvertedList));
//         }
//         inverted_lists[num_inverted_lists] = current_list;
//         num_inverted_lists++;
//     }

//     fclose(fsorted_posts);
// }
