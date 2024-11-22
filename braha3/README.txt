Web Search Engines HW3
Joe Braha and Sonali Seshadri


# Files in Submission

query_processor/processor.c - the modified BM25 query processor code
index_generator/generate_index.c - the modified BM25 index generator code
hw3/filter_docs/
    src/main.rs - the collection.tsv filtering script
    Cargo.toml - the Cargo file for the script
hw3/filter_queries.py - a script that filters the longer queries.*.tsv to a smaller
                    query file that only contains the queries from qrels.*.tsv
hw3/hw3.py - the main program that does all the query processing and evaluates results

# External dependencies

uthash  - https://troydhanson.github.io/uthash/
faiss   - https://github.com/facebookresearch/faiss
ranx    - https://github.com/amenra/ranx
h5py    - https://github.com/h5py/h5py


# Running the program

To run the project, you'll first have to prepare the files:
- run the filter_docs program on the collection.tsv to get the smaller collection
- run the BM25 index generator on the smaller collection
- fix the qrels.dev.tsv file with 
  `awk '{print $1, "0", $2, $3}' qrels.dev.tsv | columns -t > fixedqrels.dev`
- run the filter_queries script on the MSMARCO query files to get the 3 smaller query files
- run the BM25 query processor on each of the query files with `./proc -b <query_file> 100`
- install all python dependencies, probably in a conda environment
- then you can run python3 hw3.py

