!#/bin/bash

# make sure we have all the necessary files in the run/ dir:
# - collection.tsv
# - fixedqrels.dev (in MSMARCO dir)
# - sorted_queries_*

make all
make index

exe/proc -b sorted_queries_dev 500
mv query_results bm25_rank_dev

exe/proc -b sorted_queries_one 500
mv query_results bm25_rank_one

exe/proc -b sorted_queries_two 500
mv query_results bm25_rank_two

python3 ../hw3/hw3.py >eval_results.txt
