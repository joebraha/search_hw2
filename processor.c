#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uthash.h> // Include uthash header for hash table

#define MAX_WORD_SIZE (size_t)190
#define MAX_TERMS 20
#define BLOCK_SIZE (size_t)65536 // 64KB
#define MAX_BLOCKS 1000 // Maximum number of blocks for one term- need to check this


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
    int start_d_block;
    size_t start_d_offset;
    size_t start_f_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_f_offset;
    int last_did;
    int *last;
    size_t num_blocks; // Number of blocks
    UT_hash_handle hh; // Hash handle for uthash
} LexiconEntry;

// Define the hash table for the lexicon
LexiconEntry *lexicon_table = NULL;

typedef struct {
    char term[MAX_WORD_SIZE];
    int count;
    int start_d_block;
    size_t start_d_offset;
    size_t start_f_offset;
    int last_d_block;
    size_t last_d_offset;
    size_t last_f_offset;
    int last_did;
    int *last;
    size_t num_blocks; // Number of blocks
    unsigned char *compressed_d_list;
    unsigned char *compressed_f_list;
    // size_t compressed_list_size;
} PostingsList;

// get postings list from index file for each term in query
void retrieve_postings_lists(char **terms, size_t num_terms, PostingsList *postings_lists) {
    for (size_t i = 0; i < num_terms; i++) {
        // retrieving postings list for term i
        LexiconEntry *metadata = get_metadata(terms[i]);
        if (metadata) {
            size_t d_start = metadata->start_d_offset; // the start offset to be updated as we move through blocks
            size_t f_start = metadata->start_f_offset; // same but for frequency blocks 

            // allocating memory for compressed data
            postings_lists[i].compressed_d_list = malloc(BLOCK_SIZE * metadata->num_blocks);
            postings_lists[i].compressed_f_list = malloc(BLOCK_SIZE * metadata->num_blocks);
            if (!postings_lists[i].compressed_d_list || !postings_lists[i].compressed_f_list) {
                perror("Error allocating memory for compressed lists");
                exit(EXIT_FAILURE);
            }

            // to keep track of where we are writing to in postings_lists[i].compressed_<d or f>_list
            size_t d_offset = 0;
            size_t f_offset = 0;


            for (int d = metadata->start_d_block; d <= metadata->last_d_block; d+=2) {
                // read in the docids and frequencies
                int f = d + 1; // frequencies always stored in block after docids
                size_t d_block_offset = (d * BLOCK_SIZE) + d_start; // actual location to seek to 
                size_t f_block_offset = (f * BLOCK_SIZE) + f_start;
                if (d != metadata->last_d_block) {
                    // not the last block, read in from the offset to the end of the block 
                    fseek(index, d_block_offset, SEEK_SET);
                    fread((postings_lists[i].compressed_d_list + d_offset), 1, (BLOCK_SIZE - d_start), index);
                    d_offset += (BLOCK_SIZE - d_start); // incrementing by size of what we just added in 
                    d_start = 0; // reset start offset for next block

                    fseek(index, f_block_offset, SEEK_SET);
                    fread((postings_lists[i].compressed_f_list + f_offset), 1, (BLOCK_SIZE - f_start), index);
                    f_offset += (BLOCK_SIZE - f_start); // incrementing like above
                    f_start = 0; // reset start offset for next block
                } else {
                    // last or only block- only read in from start offset to last offset
                    fseek(index, d_block_offset, SEEK_SET);
                    fread((postings_lists[i].compressed_d_list + d_offset), 1, metadata->last_d_offset - d_start, index);

                    fseek(index, f_offset, SEEK_SET);  
                    fread((postings_lists[i].compressed_f_list + f_offset), 1, metadata->last_f_offset - f_start, index);
                }
            }

            // Store the postings list and metadata
            strcpy(postings_lists[i].term, terms[i]);
            postings_lists[i].count = metadata->count;
            postings_lists[i].start_d_block = metadata->start_d_block;
            postings_lists[i].start_d_offset = metadata->start_d_offset;
            postings_lists[i].start_f_offset = metadata->start_f_offset;
            postings_lists[i].last_d_block = metadata->last_d_block;
            postings_lists[i].last_d_offset = metadata->last_d_offset;
            postings_lists[i].last_f_offset = metadata->last_f_offset;
            postings_lists[i].last_did = metadata->last_did;
            
            // Copy the last array
            postings_lists[i].last = malloc(sizeof(int) * metadata->num_blocks);
            if (!postings_lists[i].last) {
                perror("Error allocating memory for last array");
                exit(EXIT_FAILURE);
            }
            memcpy(postings_lists[i].last, metadata->last, sizeof(int) * metadata->num_blocks);
            postings_lists[i].num_blocks = metadata->num_blocks;
        } else {
            printf("Term '%s' not found in lexicon\n", terms[i]);
        }
    }
}

// IMPLEMENT DAAT TRAVERSAL AND ASSOCIATED FUNCTIONS 
DAAT(postings_lists, num_terms)

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
    int count, last_did, start_d_block, last_d_block;
    int *last = malloc(sizeof(int) * MAX_BLOCKS);

    size_t start_d_offset, start_f_offset, last_f_offset, last_d_offset;

    while (fscanf(file, "%s %d %d %zu %zu %d %zu %zu %d", term, &count, &start_d_block, &start_d_offset, &start_f_offset, &last_d_block, &last_d_offset, &last_f_offset, &last_did) == 9) {
        LexiconEntry *entry = (LexiconEntry *)malloc(sizeof(LexiconEntry));
        if (!entry) {
            perror("Error allocating memory for lexicon entry");
            exit(EXIT_FAILURE);
        }
        strcpy(entry->term, term);
        entry->count = count;
        entry->start_d_block = start_d_block;
        entry->start_d_offset = start_d_offset;
        entry->start_f_offset = start_f_offset;
        entry->last_d_block = last_d_block;
        entry->last_d_offset = last_d_offset;
        entry->last_f_offset = last_f_offset;
        entry->last_did = last_did;

        // Read the variable part (last array)
        entry->last = (int *)malloc(sizeof(int) * MAX_BLOCKS);
        if (!entry->last) {
            perror("Error allocating memory for last array");
            free(entry);
            exit(EXIT_FAILURE);
        }

        entry->num_blocks = 0;
        int prev_value = -1; // Initialize to a value that is less than any valid docID
        while (fscanf(file, "%d", &entry->last[entry->num_blocks]) == 1) {
            if (entry->last[entry->num_blocks] <= prev_value) {
                break; // Stop if the current value is not greater than the previous value- last array is only increasing
            }
            prev_value = entry->last[entry->num_blocks];
            entry->num_blocks++;
            if (entry->num_blocks >= MAX_BLOCKS) {
                break;
            }
        }

        // Add the entry to the hash table
        HASH_ADD_STR(lexicon_table, term, entry);
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
        // initialize top-10 heap
        
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