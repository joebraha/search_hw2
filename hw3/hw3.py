from io import StringIO
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


def get_subset_h5(ids, embeddings, id_subset: set):
    filtered = embeddings[(ids in id_subset)]
    fids = ids[(ids in id_subset)]
    print(len(filtered))
    print(fids[:5], filtered[:5])
    return fids, filtered


# might need to adjust these appropriately based on your file directory structure
dir = "MSMARCO-Embeddings/"
file_path = dir + "msmarco_passages_embeddings_subset.h5"
queries_path = dir + "msmarco_queries_dev_eval_embeddings.h5"
query_strings_dir = dir + "queries/"
qproc = "./proc"

M = 4  # TODO: fine tune these (inc. ef_*)
k = 10  # number of nearest neighbors to fetch for each query


# calculates HNSW rankings on all queries in queries_path file, and return indicies
def calculate_hnsw(ids, embeddings, index):
    D, I = index.search(embeddings, k)
    print(ids[:5], I[:5])
    return I


def evaluate(results):
    # do MRR, Recall, NDCG x2/MAP evaluation
    ...


def get_embeddings_from_docids(docids, dids, dembeddings):
    # creates a HNSW index on only the given docids
    # d = dids[(dids in docids)]
    d = dids[np.isin(dids, docids)]
    e = dembeddings[np.isin(dids, docids)]
    # e = dembeddings[(dids in docids)]
    index = build_index(d, e)
    return index


def double_rank(qids, qembeddings, queries: dict):
    # open the file and find the query strings
    ret = {}
    for query, docids in queries.items():
        index = get_embeddings_from_docids(docids, dids, dembeddings)
        qembedded = qembeddings[(qids == query)]
        D, I = index.search(qembedded, k)
        ret[query] = I
    return ret


def build_index(ids, embeddings) -> faiss.IndexIDMap:
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


# reads file of results formatted as \n-sep "<qid> <result 1> <result 2> etc"
def read_results(file: str):
    results = {}  # dict of qid -> list of results
    with open(file) as f:
        for line in f:
            words = line.split()
            results[int(words[0])] = [int(a) for a in words[1:]]
    return results


if __name__ == "__main__":
    # before doing this: run bm25 on 10 results to 1 file, and do it on 100ish results to another file
    # bm25_rank = read_results("bm25_rank")

    qids, qembeddings = load_h5_embeddings(queries_path)
    dids, dembeddings = load_h5_embeddings(file_path)
    print(qids[:5], qembeddings[:5])

    # HNSW run
    # index = build_index(dids, dembeddings)
    # print(index)
    # vector_rank = calculate_hnsw(qids, qembeddings, index)

    bm25_big = read_results("bm25_big")
    double_rank = double_rank(qids, qembeddings, bm25_big)

    # evaluate(bm25_rank)
    evaluate(vector_rank)
    evaluate(double_rank)
