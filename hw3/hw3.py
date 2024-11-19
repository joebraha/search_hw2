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


# calculates HNSW rankings on all queries in queries_path file, and
# return dict mapping query_id -> ranked closest neighbors
def calculate_hnsw(ids, embeddings, index):
    D, I = index.search(embeddings, k)
    print(ids[:5], I[:5])
    mapped = {}
    for i in range(len(ids)):
        mapped[ids[i]] = I[i]
    return mapped


# TODO: fill in each eval method for relevant qrels (see Mehran email and Piazza for info)
def evaluate(run, qrels):
    # do MRR, Recall, NDCG x2/MAP evaluation
    ranx.evaluate(qrels, run, "ndcg@5")


def get_embeddings_from_docids(docids, dids, dembeddings):
    # creates a HNSW index on only the given docids
    # d = dids[(dids in docids)]
    d = dids[np.isin(dids, docids)]
    e = dembeddings[np.isin(dids, docids)]
    # e = dembeddings[(dids in docids)]
    index = build_index(d, e)
    return index


# returns dict of query_id -> nparray of closest k ranked neighbors
def double_rank(qids, qembeddings, queries: dict):
    # open the file and find the query strings
    ret = {}
    for query_id, docids in queries.items():
        index = get_embeddings_from_docids(docids, dids, dembeddings)
        qembedded = qembeddings[(qids == query_id)]
        D, I = index.search(qembedded, k)
        ret[query_id] = I
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


# takes the common format of query results and construct a ranx Run for it
def construct_qrels_run(query_results: dict[int, list[int]], name):
    out = {}
    for q, ranks in query_results.items():
        # l = len(ranks)
        qstr = str(q)
        out[qstr] = {}
        for i in range(len(ranks)):
            # using simple scoring scheme advised by Mehran
            out[qstr][str(ranks[i])] = 1 / (i + 1)
    for i in out:  # just for debugging
        print(i, out[i])
        return ranx.Run(out, name)


if __name__ == "__main__":
    # TODO: need to construct separate runs for each of the 3 qrels files,
    #       for each of the three methods. So 9 runs total
    #       And note will need to only do it for queries in qrels files

    # before doing this: run bm25 on 10 results to 1 file, and do it on 100ish results to another file
    bm25_rank = read_results("bm25_rank")

    qids, qembeddings = load_h5_embeddings(queries_path)
    dids, dembeddings = load_h5_embeddings(file_path)
    print(qids[:5], qembeddings[:5])

    # HNSW run
    index = build_index(dids, dembeddings)
    print(index)
    vector_rank = calculate_hnsw(qids, qembeddings, index)

    bm25_big = read_results("bm25_big")
    double_rank = double_rank(qids, qembeddings, bm25_big)

    # hit an error where qrels.dev didn't have the legacy 0 column, so added it with
    # awk '{print $1, "0", $2, $3}' qrels.dev.tsv | columns -t > fixedqrels.dev
    qrels_dev = ranx.Qrels.from_file(dir + "fixedqrels.dev", "trec")
    qrels_eval1 = ranx.Qrels.from_file(dir + "qrels.eval.one.tsv", "trec")
    qrels_eval2 = ranx.Qrels.from_file(dir + "qrels.eval.two.tsv", "trec")

    # there should at the end be 9 of these
    run1 = construct_qrels_run(bm25_rank, "bm25")
    run2 = construct_qrels_run(vector_rank, "hnsw")
    # TODO: debug, this might be empty
    run3 = construct_qrels_run(double_rank, "rerank")

    # examples:
    evaluate(run1, qrels_dev)
    evaluate(run2, qrels_dev)
    evaluate(run3, qrels_dev)
