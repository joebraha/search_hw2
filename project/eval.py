import h5py
import ranx
import faiss
import numpy as np
from typing import Dict, List


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
dir = "../project/"
file_path = dir + "embeddings/query_expansion_docs_reword_collection.txt.h5"
queries_path = dir + "embeddings/queriesdeveval.h5"
query_strings_dir = dir + "queries/"
qproc = "exe/proc"

k = 100  # number of nearest neighbors to fetch for each query


# calculates HNSW rankings on all queries in queries_path file, and
# return dict mapping query_id -> ranked closest neighbors
def calculate_hnsw(ids, embeddings, index):
    D, I = index.search(embeddings, k)
    mapped = {}
    for i in range(len(ids)):
        mapped[ids[i]] = I[i]
    return mapped


def evaluate(run, qrels, eval_or_dev):
    results = {}
    if eval_or_dev == 1:
        # dev- do not calculate NDCG- only MRR@10, Recall@100, MAP
        results["mrr@10"] = ranx.evaluate(qrels, run, "mrr@10", make_comparable=True)
        results["recall@100"] = ranx.evaluate(
            qrels, run, "recall@100", make_comparable=True
        )
        results["map"] = ranx.evaluate(qrels, run, "map", make_comparable=True)
    else:
        # eval one or two- do not calculate Recall- only MRR@10, NDCG@10, NDCG@100
        results["ndcg@10"] = ranx.evaluate(qrels, run, "ndcg@10", make_comparable=True)
        results["ndcg@100"] = ranx.evaluate(
            qrels, run, "ndcg@100", make_comparable=True
        )
        results["mrr@10"] = ranx.evaluate(qrels, run, "mrr@10", make_comparable=True)
    return results


def get_embeddings_from_docids(docids, dids, dembeddings):
    # creates a HNSW index on only the given docids
    # Filter the document IDs and embeddings to include only those in docids
    mask = np.isin(dids, docids)
    filtered_dids = dids[mask]
    filtered_embeddings = dembeddings[mask]

    if filtered_embeddings.size == 0:
        print(f"\tNo embeddings found for docids: {docids}")
        return None

    # Build the index with the filtered IDs and embeddings
    index = build_index(filtered_dids, filtered_embeddings)
    return index


# returns dict of query_id -> nparray of closest k ranked neighbors
def double_rank(qids, qembeddings, queries: Dict, dids, dembeddings):
    ret = {}
    for query_id, docids in queries.items():
        index = get_embeddings_from_docids(docids, dids, dembeddings)
        if index is None:
            print(f"\tSkipping query_id {query_id} due to empty index.")
            continue
        qembedded = qembeddings[np.where(qids == str(query_id))]
        if qembedded.size == 0:
            print(f"\tNo embedding found for query_id {query_id}")
            continue

        D, I = index.search(qembedded, k)
        if I.size == 0:
            print(f"\tNo nearest neighbors found for query_id {query_id}")
            continue
        ret[query_id] = I[0].tolist()
    return ret


def build_index(
    ids, embeddings, m=8, ef_construction=200, ef_search=200
) -> faiss.IndexIDMap:
    print("\t* Building index...")
    dim = len(embeddings[0])
    print(f"\t\tDimension: {dim}")

    # Create the HNSW index
    hnsw_index = faiss.IndexHNSWFlat(dim, m)
    hnsw_index.hnsw.efConstruction = ef_construction
    hnsw_index.hnsw.efSearch = ef_search

    # Wrap the HNSW index with IndexIDMap
    index = faiss.IndexIDMap(hnsw_index)

    print(f"\t\tTrained: {index.is_trained}")
    print(f"\t\tAdding {len(ids)} documents to index...")
    index.add_with_ids(embeddings, ids)
    # print("\t\tIndex built.")
    print(f"\t\tNumber of documents: {index.ntotal}")
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
def construct_qrels_run(query_results: Dict[int, List[int]], name: str):
    out = {}
    for q, ranks in query_results.items():
        # l = len(ranks)
        qstr = str(q)
        out[qstr] = {}
        for i in range(len(ranks)):
            # using simple scoring scheme advised by Mehran
            out[qstr][str(ranks[i])] = 1 / (i + 1)
    return ranx.Run(out, name)


def load_query_ids(query_file):
    with open(query_file) as f:
        return [line.split()[0] for line in f]


def filter_embeddings_by_queries(qids, qembeddings, query_ids):
    mask = np.isin(qids, query_ids)
    return qids[mask], qembeddings[mask]


def print_results(results):
    for k, v in results.items():
        print(f"{k}: {v}")


def write_results_to_file(file_path, results_dict):
    with open(file_path, "w") as f:
        for section, results in results_dict.items():
            f.write(f"{section}:\n")
            for method, result in results.items():
                f.write(method + ": " + str(result) + "\n")
            f.write("\n")


if __name__ == "__main__":
    print(
        "EVALUATION FOR... document rewording expansion for BM25@100 vs original HNSW vs original rerank@500"
    )
    # Load embeddings for all queries to be evaluated
    qids, qembeddings = load_h5_embeddings(queries_path)

    # Load query IDs from each query file
    print("Loading query IDs...")
    query_ids_dev = load_query_ids("sorted_queries_dev")
    query_ids_one = load_query_ids("sorted_queries_one")
    query_ids_two = load_query_ids("sorted_queries_two")

    # Filter embeddings for each set of queries
    print("Filtering embeddings...")
    qids_dev, qembeddings_dev = filter_embeddings_by_queries(
        qids, qembeddings, query_ids_dev
    )
    qids_one, qembeddings_one = filter_embeddings_by_queries(
        qids, qembeddings, query_ids_one
    )
    qids_two, qembeddings_two = filter_embeddings_by_queries(
        qids, qembeddings, query_ids_two
    )

    # Load embeddings for all documents in subset of collection
    print("Loading document embeddings...")
    dids, dembeddings = load_h5_embeddings(file_path)
    print(qids[:5], qembeddings[:5])

    # Generate BM25 results for top 100 results for each query file
    print("\nGenerating BM25 results for document rewording method...")
    bm25_rank_dev = read_results("bm25_rank_dev")
    bm25_rank_one = read_results("bm25_rank_one")
    bm25_rank_two = read_results("bm25_rank_two")

    # HNSW run for each set of queries
    print("\nBuilding HNSW index...")
    index = build_index(dids, dembeddings)
    print("\nCalculating HNSW rankings...")
    vector_rank_dev = calculate_hnsw(qids_dev, qembeddings_dev, index)
    vector_rank_one = calculate_hnsw(qids_one, qembeddings_one, index)
    vector_rank_two = calculate_hnsw(qids_two, qembeddings_two, index)

    # Combined reranking results for each set of queries
    print("\nCalculating rerank rankings...")
    print("for DEV")
    # read in BM25@500 for dev
    # bm25_dev_500 = read_results("bm25_rank_dev")
    double_rank_dev = double_rank(
        qids_dev, qembeddings_dev, bm25_rank_dev, dids, dembeddings
    )

    print("\nfor EVAL ONE")
    # read in BM25@500 for one
    # bm25_one_500 = read_results("10k_orig_query_results_one_500")
    double_rank_one = double_rank(
        qids_one, qembeddings_one, bm25_rank_one, dids, dembeddings
    )

    print("\nfor EVAL TWO")
    # read in BM25@500 for two
    # bm25_two_500 = read_results("10k_orig_query_results_two_500")
    double_rank_two = double_rank(
        qids_two, qembeddings_two, bm25_rank_two, dids, dembeddings
    )

    # Load qrels files
    # hit an error where qrels.dev didn't have the legacy 0 column, so added it with
    # awk '{print $1, "0", $2, $3}' qrels.dev.tsv | columns -t > fixedqrels.dev
    print("Loading qrels files...")
    qrels_dev = ranx.Qrels.from_file(dir + "qrels/fixedqrels.dev", "trec")
    qrels_eval1 = ranx.Qrels.from_file(dir + "qrels/qrels.eval.one.tsv", "trec")
    qrels_eval2 = ranx.Qrels.from_file(dir + "qrels/qrels.eval.two.tsv", "trec")

    # Construct runs for each method and each set of queries
    print("Constructing runs...")
    # DEV
    bm25_dev = construct_qrels_run(bm25_rank_dev, "bm25")
    hnsw_dev = construct_qrels_run(vector_rank_dev, "hnsw")
    rerank_dev = construct_qrels_run(double_rank_dev, "rerank")

    # EVAL ONE
    bm25_one = construct_qrels_run(bm25_rank_one, "bm25")
    hnsw_one = construct_qrels_run(vector_rank_one, "hnsw")
    rerank_one = construct_qrels_run(double_rank_one, "rerank")

    # EVAL TWO
    bm25_two = construct_qrels_run(bm25_rank_two, "bm25")
    hnsw_two = construct_qrels_run(vector_rank_two, "hnsw")
    rerank_two = construct_qrels_run(double_rank_two, "rerank")

    # Evaluate each run

    # Define the file path for the results
    results_file = dir + "eval_results/queries_and_reword_results"

    # qrels.dev
    print("Evaluating runs for dev...")
    dev_bm25_results = evaluate(bm25_dev, qrels_dev, 1)
    dev_hnsw_results = evaluate(hnsw_dev, qrels_dev, 1)
    dev_rerank_results = evaluate(rerank_dev, qrels_dev, 1)

    # qrels.eval.one
    print("Evaluating runs for eval one...")
    eval1_bm25_results = evaluate(bm25_one, qrels_eval1, 2)
    eval1_hnsw_results = evaluate(hnsw_one, qrels_eval1, 2)
    eval1_rerank_results = evaluate(rerank_one, qrels_eval1, 2)

    # qrels.eval.two
    print("Evaluating runs for eval two...")
    eval2_bm25_results = evaluate(bm25_two, qrels_eval2, 2)
    eval2_hnsw_results = evaluate(hnsw_two, qrels_eval2, 2)
    eval2_rerank_results = evaluate(rerank_two, qrels_eval2, 2)

    # Collect all results in a dictionary
    results_dict = {
        "Dev BM25 results": dev_bm25_results,
        "Dev HNSW results": dev_hnsw_results,
        "Dev rerank results": dev_rerank_results,
        "Eval one BM25 results": eval1_bm25_results,
        "Eval one HNSW results": eval1_hnsw_results,
        "Eval one rerank results": eval1_rerank_results,
        "Eval two BM25 results": eval2_bm25_results,
        "Eval two HNSW results": eval2_hnsw_results,
        "Eval two rerank results": eval2_rerank_results,
    }

    # Write all results to a single file
    write_results_to_file(results_file, results_dict)

    # # Print results
    # print("\nDev BM25 results:")
    # print_results(dev_bm25_results)
    # print("\nDev HNSW results:")
    # print_results(dev_hnsw_results)
    # print("\nDev rerank results:")
    # print_results(dev_rerank_results)

    # print("\nEval one BM25 results:")
    # print_results(eval1_bm25_results)
    # print("\nEval one HNSW results:")
    # print_results(eval1_hnsw_results)
    # print("\nEval one rerank results:")
    # print_results(eval1_rerank_results)

    # print("\nEval two BM25 results:")
    # print_results(eval2_bm25_results)
    # print("\nEval two HNSW results:")
    # print_results(eval2_hnsw_results)
    # print("\nEval two rerank results:")
    # print_results(eval2_rerank_results)
