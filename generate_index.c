#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_TERM_COUNT 1000
// need to change this ^ to match the number of terms in the dataset (get from length of words file?)
#define INITIAL_ENTRY_SIZE 100
// need to figure out what's reasonable for this

typedef struct {
    int doc_id;
    int length;
} DocEntry;

typedef struct {
    char *word;
    int count;
    int doc_id;
} Posting;

typedef struct {
    char *word;
    int total_count;
} WordEntry;

typedef struct {
    int doc_id;
    int frequency;
} InvertedListEntry;

typedef struct {
    char *word;
    InvertedListEntry *entries;
    int entry_count;
    int entry_capacity;
} InvertedList;

DocEntry *doc_table = NULL;
WordEntry *word_table = NULL;
InvertedList *inverted_lists = NULL;
// ^ this is actually the full inverted index technically, before we do all the compression and storage stuff

int num_docs = 0;
int num_words = 0;
int num_inverted_lists = 0;
int inverted_list_capacity = 0;

void read_doc_and_word_files();
void build_inverted_lists();
void compress_and_store_inverted_lists();
void build_lexicon_and_page_table();
void store_meta_information();


int main() {
    // steps 1-3 are basically doing merge sort

    // step 1: split the file into smaller chunks
    system("split -l 1000000 testout_posts.txt chunk_");

    // step 2: sort each chunk
    system("for file in chunk_*; do sort -k1,1 \"$file\" -o \"$file.sorted\"; done");

    // step 3: merge sorted chunks
    system("sort -m -k1,1 chunk_*.sorted -o sorted_posts.txt");

    // step 4: read doc and word files
    read_doc_and_word_files();

    // step 5: build inverted lists
    build_inverted_lists();

    // step 6: compress and store inverted lists
    compress_and_store_inverted_lists();

    // step 7: build lexicon and page table
    build_lexicon_and_page_table();

    // step 8: store meta information
    store_meta_information();

    return 0;
}

void read_doc_and_word_files() {
    FILE *fdocs = fopen("testout_docs.txt", "r");
    FILE *fwords = fopen("testout_words.txt", "r");

    if (!fdocs || !fwords) {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }

    // read testout_docs.txt into doc_table, the array of DocEntry structs
    while (!feof(fdocs)) {
        doc_table = realloc(doc_table, (num_docs + 1) * sizeof(DocEntry));
        fscanf(fdocs, "%d %d\n", &doc_table[num_docs].doc_id, &doc_table[num_docs].length);
        num_docs++;
    }
    fclose(fdocs);

    // read testout_words.txt into word_table, the array of WordEntry structs
    while (!feof(fwords)) {
        word_table = realloc(word_table, (num_words + 1) * sizeof(WordEntry));
        word_table[num_words].word = malloc(256 * sizeof(char)); // Assuming max word length is 255
        fscanf(fwords, "%s %d\n", word_table[num_words].word, &word_table[num_words].total_count);
        num_words++;
    }
    fclose(fwords);
}

void build_inverted_lists() {
    FILE *fsorted_posts = fopen("sorted_posts.txt", "r");
    if (!fsorted_posts) {
        perror("Error opening sorted_posts.txt");
        exit(EXIT_FAILURE);
    }

    // initial allocation for inverted_lists
    inverted_list_capacity = INITIAL_TERM_COUNT;
    inverted_lists = malloc(inverted_list_capacity * sizeof(InvertedList));

    char current_word[256];
    InvertedList current_list;
    current_list.word = NULL;
    current_list.entries = NULL;
    current_list.entry_count = 0;
    current_list.entry_capacity = 0; // initialize capacity

    while (!feof(fsorted_posts)) {
        char word[256];
        int count, doc_id;
        fscanf(fsorted_posts, "%s %d %d\n", word, &count, &doc_id);

        if (current_list.word == NULL || strcmp(current_list.word, word) != 0) {
            // Save the current list if it exists
            if (current_list.word != NULL) {
                if (num_inverted_lists >= inverted_list_capacity) {
                    inverted_list_capacity *= 2;
                    inverted_lists = realloc(inverted_lists, inverted_list_capacity * sizeof(InvertedList));
                }
                inverted_lists[num_inverted_lists] = current_list;
                num_inverted_lists++;
            }

            // start a new list
            current_list.word = strdup(word);
            current_list.entries = malloc(INITIAL_ENTRY_SIZE * sizeof(InvertedListEntry));
            current_list.entry_count = 0;
            current_list.entry_capacity = INITIAL_ENTRY_SIZE;
        }

        // add the posting to the current list
        if (current_list.entry_count >= current_list.entry_capacity) {
            current_list.entry_capacity *= 2;
            current_list.entries = realloc(current_list.entries, current_list.entry_capacity * sizeof(InvertedListEntry));
        }
        current_list.entries[current_list.entry_count].doc_id = doc_id;
        current_list.entries[current_list.entry_count].frequency = count;
        current_list.entry_count++;
    }

    // save the last list
    if (current_list.word != NULL) {
        if (num_inverted_lists >= inverted_list_capacity) {
            inverted_list_capacity *= 2;
            inverted_lists = realloc(inverted_lists, inverted_list_capacity * sizeof(InvertedList));
        }
        inverted_lists[num_inverted_lists] = current_list;
        num_inverted_lists++;
    }

    fclose(fsorted_posts);
}

void compress_and_store_inverted_lists() {
    // implement var byte compression and storage logic here
}

void build_lexicon_and_page_table() {
    // implement lexicon and page table building logic here
}

void store_meta_information() {
    // implement meta informationb storage logic here
}