#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uthash.h> // Include uthash header for hash table

#define MAX_WORD_SIZE (size_t)190
#define MAX_TERMS 20

// Define constants for search modes
#define CONJUNCTIVE 1
#define DISJUNCTIVE 2

// Function prototypes
void load_lexicon(const char *filename);
LexiconEntry *get_metadata(const char *term);
void DAAT();
void retrieve_postings_lists(char **terms, size_t num_terms, PostingsList *postings_lists);
void parse_term(char *term);

// Define the structure for lexicon entries
typedef struct {
    char term[MAX_WORD_SIZE];
    int count;
    size_t block_index;
    size_t offset;
    size_t last_did_block;
    int last_did;
    size_t last_did_offset;
    UT_hash_handle hh; // Hash handle for uthash
} LexiconEntry;

// Define the hash table for the lexicon
LexiconEntry *lexicon_table = NULL;

typedef struct {
    char term[MAX_WORD_SIZE];
    size_t block_index;
    size_t offset;
    size_t last_did_block;
    int last_did;
    size_t last_did_offset;
    unsigned char *compressed_list;
    size_t compressed_list_size;
} PostingsList;

// get postings list from index file for each term in query
// *** NOT DONE - I don't think this would actually fetch the frequencies- need to adjust 
void retrieve_postings_lists(char **terms, size_t num_terms, PostingsList *postings_lists) {
    for (size_t i = 0; i < num_terms; i++) {
        LexiconEntry *metadata = get_metadata(terms[i]);
        if (metadata) {
            // Seek to the starting block in the index file
            fseek(index, metadata->block_index, SEEK_SET);

            // Read the compressed postings list
            size_t compressed_list_size = metadata->last_did_offset - metadata->offset;
            unsigned char *compressed_list = (unsigned char *)malloc(compressed_list_size);
            if (!compressed_list) {
                perror("Error allocating memory for compressed list");
                exit(EXIT_FAILURE);
            }
            fread(compressed_list, 1, compressed_list_size, index);

            // Store the postings list and metadata
            strcpy(postings_lists[i].term, terms[i]);
            postings_lists[i].block_index = metadata->block_index;
            postings_lists[i].offset = metadata->offset;
            postings_lists[i].last_did_block = metadata->last_did_block;
            postings_lists[i].last_did = metadata->last_did;
            postings_lists[i].last_did_offset = metadata->last_did_offset;
            postings_lists[i].compressed_list = compressed_list;
            postings_lists[i].compressed_list_size = compressed_list_size;
        } else {
            printf("Term '%s' not found in lexicon\n", terms[i]);
        }
    }
}

// IMPLEMENT DAAT TRAVERSAL AND ASSOCIATED FUNCTIONS 

// function to retrieve metadata from lexicon
LexiconEntry *get_metadata(const char *term) {
    LexiconEntry *entry;
    HASH_FIND_STR(lexicon_table, term, entry); // Find entry in hash table
    return entry;
}
// load lexicon into memory for easy search of term metadata
void load_lexicon(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening lexicon file");
        exit(EXIT_FAILURE);
    }

    char term[MAX_WORD_SIZE];
    int count, last_did;
    size_t block_index, offset, last_did_block, last_did_offset;

    while (fscanf(file, "%s %d %zu %zu %zu %d %zu\n", term, &count, &block_index, &offset, &last_did_block, &last_did, &last_did_offset) != EOF) {
        LexiconEntry *entry = (LexiconEntry *)malloc(sizeof(LexiconEntry));
        if (!entry) {
            perror("Error allocating memory for lexicon entry");
            exit(EXIT_FAILURE);
        }
        strcpy(entry->term, term);
        entry->count = count;
        entry->block_index = block_index;
        entry->offset = offset;
        entry->last_did = last_did;
        entry->last_did_block = last_did_block;
        entry->last_did_offset = last_did_offset;
        HASH_ADD_STR(lexicon_table, term, entry); // Add entry to hash table
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
            // checks if character is not alphbetic, makes lowercase, moves to next character
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


    // fetch search mode from command line interface
    // fetch full query from command line interface
    char search_mode_input[10];
    char query[1024];

    while (1) {
        // Prompt for search mode
        printf("Enter search mode (conjunctive/disjunctive): ");
        if (fgets(search_mode_input, sizeof(search_mode_input), stdin) == NULL) {
            break; // Exit on EOF or error
        }
        search_mode_input[strcspn(search_mode_input, "\n")] = '\0'; // Remove newline character

        // Determine search mode
        int search_mode;
        if (strcasecmp(search_mode_input, "conjunctive") == 0) {
            search_mode = CONJUNCTIVE;
        } else if (strcasecmp(search_mode_input, "disjunctive") == 0) {
            search_mode = DISJUNCTIVE;
        } else {
            printf("Invalid search mode. Please enter 'conjunctive' or 'disjunctive'.\n");
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
        char *term = strtok(query, " \t\n\r\f\v!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        while (term != NULL && num_terms < MAX_TERMS) {
            parse_term(term);
            if (strlen(term) > 0) {
                terms[num_terms++] = strdup(term);
            }
            term = strtok(NULL, " \t\n\r\f\v!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        }

        // Retrieve postings lists for all terms in query
        PostingsList postings_lists[MAX_TERMS];
        retrieve_postings_lists(terms, num_terms, postings_lists);

        // Perform DAAT traversal - HAVENT TOUCHED THIS YET
        // need to create heap for storing top 10 results
        // need to implement open_list, close_list, nextGEQ, and get_score functions to do DAAT
        DAAT(postings_lists, num_terms);

        // Free allocated memory for terms and postings lists
        for (size_t i = 0; i < num_terms; i++) {
            free(terms[i]);
            free(postings_lists[i].compressed_list);
        }
    }

    // Close index file
    fclose(index);

    return 0;
}




// retrieve postings lists for all terms in query
    // get metadata for term from lexicon
    // seek to starting block in index file
    // read in block/s containing postings list, still in compressed form

// create heap for storing top 10 results of docIDs/scores

// traverse lists in DAAT fashion, using BM25, and only uncompressing as needed