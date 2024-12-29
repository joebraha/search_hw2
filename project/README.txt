Web Search Engines Final Project
Joe Braha and Sonali Seshadri


# Files in Submission

expanded_docs - all the document files used, including all queries, the original 10k documents, and all the expanded document collections
embeddings/ - h5 embeddings of vector collections for the above documents
eval_results/ - the results of running the evaluation on each of the document collections
qrels/ - all the qrels files, same as used in hw3
queries/ - the queries used, same as used in hw3
document_encoder.py - the script used to encode the document collections as vectors and generate h5 embeddings
eval.py - the script that performs all the evaluations for results; slightly modified from hw3
local_ollama.py - the script used to generate the document expansions with ollama
run_eval.sh - a shell script to streamline the evaluation process, which should be run from the /run/ dir of the HW2 project


# External Dependencies

uthash  - https://troydhanson.github.io/uthash/
faiss   - https://github.com/facebookresearch/faiss
ranx    - https://github.com/amenra/ranx
h5py    - https://github.com/h5py/h5py
sentence_transformers - https://huggingface.co/sentence-transformers/msmarco-MiniLM-L6-cos-v5


# Running the Project

The included files can be used with the search system from HW2, by just running run_eval.sh from in the /run/ directory,
and swapping the appropriate document collection into the directory from expanded_docs/, and making sure the the paths
in eval.py align with your system. If you want to regenerate the expansions or embeddings, that can be done with the 
included scripts.
