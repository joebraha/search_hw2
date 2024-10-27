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
UTHASH=../../repos/uthash/src/
WARNINGS=-Wall -Wextra

gen: dir_check ../index_generator/generate_index.c
	gcc -I $(UTHASH) ../index_generator/generate_index.c -o exe/gen

wgen: dir_check ../index_generator/generate_index.c
	gcc $(WARNINGS) -I $(UTHASH) ../index_generator/generate_index.c -o exe/gen
		
proc: dir_check ../query_processor/processor.c
	gcc -I $(UTHASH) ../query_processor/processor.c -o exe/proc

wproc: dir_check ../query_processor/processor.c
	gcc $(WARNINGS) -I $(UTHASH) ../query_processor/processor.c -o exe/proc

parse: dir_check ../parser/src/main.rs
	cargo build --manifest-path ../parser/Cargo.toml -r 
	cp ../parser/target/release/parse exe/

all: parse gen proc

index: parse gen
	./exe/parse
	sort --version-sort -S 2G -o sorted_posts posts_out.txt
	./exe/gen sorted_posts

run: index proc
	./exe/proc

dir_check:
	mkdir -p exe

clean:
	rm exe/*

