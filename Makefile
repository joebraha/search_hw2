# replace with your path to uthash dir
UTHASH=../repos/uthash/src/
WARNINGS=-Wall -Wextra

gen: generate_index.c
	gcc generate_index.c -o gen

wgen: generate_index.c
	gcc $(WARNINGS) generate_index.c -o gen
		
proc: processor.c
	gcc -I $(UTHASH) processor.c -o proc

wproc: processor.c
	gcc $(WARNINGS) -I $(UTHASH) processor.c -o proc

parse: parser/src/main.rs
	cargo build --manifest-path parser/Cargo.toml -r 
	cp parser/target/release/parse .

all: parse gen proc

wall: wgen wproc

clean:
	rm parse gen proc
