import h5py
import ranx
import faiss
import numpy as np
import subprocess


def load_h5_embeddings(file_path, id_key="id", embedding_key="embedding"):
    """
    Load IDs and embeddings from an HDF5 file.

    Parameters:
    - id_key: Dataset name for the IDs inside the HDF5 file.
    - embedding_key: Dataset name for the embeddings inside the HDF5 file.

    Returns:
    - ids: Numpy array of IDs (as strings).
    - embeddings: Numpy array of embeddings (as float32).
    """
    print(f"Loading data from {file_path}...")
    with h5py.File(file_path, "r") as f:
        ids = np.array(f[id_key]).astype(str)
        embeddings = np.array(f[embedding_key]).astype(np.float32)

    print(f"Loaded {len(ids)} embeddings.")
    return ids, embeddings


# might need to adjust these appropriately based on your file directory structure
dir = "MSMARCO-Embeddings/"
file_path = dir + "msmarco_passages_embeddings_subset.h5"
queries_path = dir + "msmarco_queries_dev_eval_embeddings.h5"
query_strings_dir = dir + "queries/"
qproc = "./proc"

M = 4  # TODO: fine tune these (inc. ef_*)
k = 10  # number of nearest neighbors to fetch for each query


# calculates HNSW rankings on all queries in queries_path file, and return indicies
def calculate_hnsw(queries_path: str, index):
    ids, query_embeddings = load_h5_embeddings(queries_path)
    D, I = index.search(query_embeddings, k)
    print(ids[:5], I[:5])
    return I


def evaluate(results):
    # do MRR, Recall, NDCG x2/MAP evaluation
    ...


def call_bm25(
    queries: list[tuple[int, str]], program_name, num_results
) -> list[tuple[int, list[int]]]:
    """calls the BM25 index from program_name on each of queries and returns
    a list of tuples with num_results docids per query"""

    # for every query:
    # call the bm25 processor with a query
    # put the resulting docids in a list
    # return the list

    print("running queries...")
    qtype = "d"
    proc = subprocess.Popen(
        program_name, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    fullqstring = f"{num_results}\n"
    for query in queries:
        # out = proc.communicate(f"{num_results}\n{qtype}\n{query[1]}".encode())
        fullqstring += f"{qtype}\n{query[1]}"
    out = proc.communicate(fullqstring)
    if out[1]:
        raise Exception(out[1])
    out = out[0]

    # loop to parse the results from proc while keeping track of which query we're up to
    query_results = []
    current_query = 0
    started_new_query = False
    current_results = []
    for line in out:
        if line[0] <= "9":  # then it's a result, not text
            words = line.split()
            print(words)
            current_results.append((int(words[2][:-1])))
        elif started_new_query:
            started_new_query = False
            print(current_results)
            query_results.append((queries[current_query][0], current_results))
            current_results = []
            current_query += 1

    # iterout = out.splitlines()[5:]
    # for i in range(num_results):
    #     if i > len(iterout) - 1:
    #         break
    #     line = iterout[i]
    #     if not line:
    #         break
    #     words = line.split()
    #     # print(words)
    #     # query_results.append((int(word[2][:-1]), float(word[4])))
    #     query_results.append((int(words[2][:-1])))

    return query_results


def get_embeddings_from_docids(docids):
    # creates a HNSW index on only the given docids
    ...


def rerank(queries, docids, index):  # TODO:
    query_embeddings = ...  # get embeddings from queries
    doc_embeddings = get_embeddings_from_docids(docids)
    D, I = index.search(query_embeddings, k)
    return I


def double_rank(queries, index):
    # open the file and find the query strings
    initial_results = call_bm25(
        queries, qproc, 20
    )  # TODO: fine tune number of results to get
    print(initial_results)
    result = rerank(queries, initial_results, index)
    return result


def build_index() -> faiss.IndexIDMap:
    ids, embeddings = load_h5_embeddings(file_path)
    dim = len(embeddings[0])
    print(f"Dimension: {dim}")
    index = faiss.IndexIDMap(faiss.IndexHNSWFlat(dim, M))
    print(f"Trained: {index.is_trained}")
    print("Adding documents to index...")
    index.add_with_ids(embeddings, ids)
    print(f"Number of documents: {index.ntotal}")
    return index


def get_query_strings(query_strings):
    strings = []
    with open(query_strings) as f:
        for line in f:
            ws = line.split(maxsplit=1)
            strings.append([int(ws[0]), ws[1]])
    return strings


def get_all_query_strings():
    print("Loading query strings...")
    strings = (
        get_query_strings(query_strings_dir + "queries.dev.tsv")
        + get_query_strings(query_strings_dir + "queries.train.tsv")
        + get_query_strings(query_strings_dir + "queries.eval.tsv")
    )
    return strings


if __name__ == "__main__":
    bm25_rank = call_bm25(get_all_query_strings(), qproc, 10)
    print(bm25_rank)

    index = build_index()
    print(index)

    vector_rank = calculate_hnsw(queries_path, index)
    double_rank = double_rank("", index)

    evaluate(bm25_rank)
    evaluate(vector_rank)
    evaluate(double_rank)
