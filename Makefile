# Makefile to streamline development and assist in deployment
# all binaries are put in the run/ directory, while output files are left in the root dir
#
# gen/proc/parse - compile the respective programs (make all to make all 3)
# wgen/wproc - compile with all warnings
# clean - deletes the binaries
#
# index - does all the processing to generate the inverted index and other files
# 			- runs the parser, sorts the postings, and runs the index generator
# run - runs the query processor, and builds the index if necessary


# replace with your path to uthash dir
UTHASH=../repos/uthash/src/
WARNINGS=-Wall -Wextra

gen: index_generator/generate_index.c
	gcc -I $(UTHASH) index_generator/generate_index.c -o run/gen

wgen: index_generator/generate_index.c
	gcc $(WARNINGS) -I $(UTHASH) index_generator/generate_index.c -o run/gen
		
proc: query_processor/processor.c
	gcc -I $(UTHASH) query_processor/processor.c -o run/proc

wproc: query_processor/processor.c
	gcc $(WARNINGS) -I $(UTHASH) query_processor/processor.c -o run/proc

parse: parser/src/main.rs
	cargo build --manifest-path parser/Cargo.toml -r 
	cp parser/target/release/parse run/

all: parse gen proc

index: parse gen
	./run/parse
	sort --version-sort -S 2G -o sorted_posts posts_out.txt
	./run/gen sorted_posts

run: index proc
	./run/proc

clean:
	rm run/*

