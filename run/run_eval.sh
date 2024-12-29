!#/bin/bash -e

# make sure we have all the necessary files in the run/ dir:
# - collection.tsv
# - fixedqrels.dev (in MSMARCO dir)
# - sorted_queries_*

make all
make index

exe/proc -b sorted_queries_dev 300
mv query_results bm25_rank_dev

exe/proc -b sorted_queries_one 300
mv query_results bm25_rank_one

exe/proc -b sorted_queries_two 300
mv query_results bm25_rank_two

python3 ../project/eval.py
