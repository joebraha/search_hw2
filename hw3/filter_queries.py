# script to filter queries file based on the qrels file provided

from sys import argv


def filter(queries_file, qrels_file, file_out):
    qrels_queries = set()
    with open(qrels_file) as f:
        for line in f:
            words = line.split()
            # print(words[0])
            if not words[0] in qrels_queries:
                qrels_queries.add(words[0])

    fout = open(file_out, "w")
    with open(queries_file) as f:
        for line in f:
            words = line.split()
            # print(words)
            if words[0] in qrels_queries:
                fout.write(line)


if __name__ == "__main__":
    if len(argv) < 4:
        print("Usage: filter <queries_file> <qrels_file> <output_file>")
        exit(0)
    filter(argv[1], argv[2], argv[3])

"""
Ran on queries.dev.tsv and queries.eval.tsv to generate 
filtered_queries.dev, filtered_queries.one, filtered_queries.two,
then sorted with sort filtered_queries.<each> -o sorted_queries_<each>
"""
