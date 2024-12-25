from sentence_transformers import SentenceTransformer
import h5py
import numpy as np
import sys
# import pandas as pd

# filepath = "collection.tsv"

if len(sys.argv) < 1:
    print("usage: encoder.py <source file of <id sentence> pairs>")
    exit(1)

filepath = sys.argv[1]

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
# df_describe = pd.DataFrame(doc_emb)
# print(df_describe.describe())
ids = np.array(ids, dtype=dt)
# print(ids)

print("writing documents to h5 file...")

with h5py.File("embedded_docs.h5", "w") as f:
    e = f.create_dataset("embedding", (10, 384), "<f2", data=doc_emb)
    i = f.create_dataset("id", (10,), data=ids)
