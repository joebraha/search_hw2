from sentence_transformers import SentenceTransformer
import h5py
import numpy as np
import sys
# import pandas as pd

# filepath = "collection.tsv"
files = [
    "docs_reword_collection.txt",
    "query_expansion_collection.txt",
    "query_expansion_docs_reword_collection.txt",
    "random_filtered_docs.txt",
    "queriesdeveval",
]

# if len(sys.argv) < 1:
#     print("usage: encoder.py <source file of <id sentence> pairs>")
#     exit(1)

# filepath = sys.argv[1]


def encode_file(filepath, fout):
    # pull out the docids from each line
    docs = [str(line.split()[1:]) for line in open(filepath)]
    ids = [line.split()[0] for line in open(filepath)]

    # Load the model
    model = SentenceTransformer("sentence-transformers/msmarco-MiniLM-L6-cos-v5")
    doc_emb = model.encode(
        docs,
        batch_size=16,
        show_progress_bar=True,
        convert_to_numpy=True,
        # device="cuda",
        max_length=512,
        truncation=True,
    )

    dt = h5py.special_dtype(vlen=str)

    doc_emb = np.array(doc_emb)
    ids = np.array(ids, dtype=dt)

    print(f"writing documents in {filepath} to h5 file...")

    filelen = 202185 if filepath == "queriesdeveval" else 10000
    with h5py.File(fout, "w") as f:
        f.create_dataset("embedding", (filelen, 384), "<f2", data=doc_emb)
        f.create_dataset("id", (filelen,), data=ids)


if __name__ == "__main__":
    for f in files:
        encode_file("expanded_docs/" + f, "embeddings/" + f + ".h5")
