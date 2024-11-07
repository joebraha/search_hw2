import h5py
import faiss
import numpy as np


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


file_path = "msmarco_passages_embeddings_subset.h5"
queries_path = "msmarco_queries_dev_eval_embeddings.h5"
M = 4  # TODO: fine tune these (inc. ef_*)
k = 10  # number of nearest neighbors to fetch for each query


# calculates HNSW rankings on all queries in queries_path file, and return indicies
def calculate_hnsw(queries_path: str, index):
    ids, query_embeddings = load_h5_embeddings(queries_path)
    D, I = index.search(query_embeddings, k)
    print(ids[:5], I[:5])
    return I


def call_bm25(queries):
    # for every query:
    # call the bm25 processor with a query
    # put the resulting docids in a list
    # return the list
    ...


def rerank(docids): ...


def double_rank(queries):
    # open the file and find the query strings
    docids = call_bm25(queries)
    result = rerank(docids)
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


# faiss.IndexIDMap()

if __name__ == "__main__":
    index = build_index()
    print(index)
    vector_rank = calculate_hnsw(queries_path, index)
    double_rank = double_rank("")
