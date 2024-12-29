import h5py
import numpy as np

def load_document_ids(file_path):
    with open(file_path, 'r') as f:
        document_ids = [line.split('\t')[0] for line in f]
    print(f"Total document IDs loaded for filtering: {len(document_ids)}")
    print("Sample document ID:" + str(document_ids[0]))
    return document_ids

def filter_embeddings(h5_file_path, document_ids, output_file_path, id_key="id", embedding_key="embedding"):
    with h5py.File(h5_file_path, 'r') as h5_file:
        ids = np.array(h5_file[id_key]).astype(str)
        embeddings = np.array(h5_file[embedding_key]).astype(np.float32)
        
        # Convert document_ids to the same type as ids
        document_ids = np.array(document_ids, dtype=ids.dtype)

        # Debug: Print data types and sample values
        print(f"Data type of IDs in HDF5 file: {ids.dtype}")
        print(f"Data type of document IDs: {document_ids.dtype}")
        print("Sample ID from HDF5 file:", ids[0])
        print("Sample document ID after conversion:", document_ids[0])
        
        # Create a mask to filter the embeddings
        mask = np.isin(ids, document_ids)
        
        filtered_ids = ids[mask]
        filtered_embeddings = embeddings[mask]
        
        print(f"Total IDs in original file: {len(ids)}")
        print(f"Total embeddings in original file: {len(embeddings)}")
        print(f"Total IDs after filtering: {len(filtered_ids)}")
        print(f"Total embeddings after filtering: {len(filtered_embeddings)}")

        # Convert filtered_ids to fixed-length ASCII strings
        filtered_ids = np.char.encode(filtered_ids, 'ascii', 'ignore')
        
        # Save the filtered embeddings to a new HDF5 file
        with h5py.File(output_file_path, 'w') as output_file:
            output_file.create_dataset(id_key, data=filtered_ids)
            output_file.create_dataset(embedding_key, data=filtered_embeddings)

# Load the document IDs from random_filtered_docs.txt
document_ids = load_document_ids('random_filtered_docs.txt')

# Filter the embeddings and save to a new HDF5 file
filter_embeddings('msmarco_passages_embeddings_subset.h5', document_ids, 'filtered_embeddings.h5')