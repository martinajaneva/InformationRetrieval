import gzip
import pickle
from process_text import processing_tokenize
from collections import defaultdict, Counter
import os

"""
Inverted index class.
Functions inspired by: 
https://mocilas.github.io/2015/11/18/Python-Inverted-Index-for-dummies/,
https://shivammehta25.github.io/posts/your-own-mini-google-search-inverted-indexes-and-boolean-retrieval/
"""

"""
Builds inverted index from a single text file by keeping track of the file name of token.
Args:
    file_path (str): The file path to the single text data.
Returns:
    inverted_index (defaultdict)
"""
def inverted_index_build(file_path):
    inverted_index = defaultdict(list)
    filename = file_path.name

    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()
        tokens = processing_tokenize(text)
        term_counter = Counter(tokens)
        
        for token in term_counter:
            inverted_index[token].append(filename)

    return inverted_index

def save_inverted_index(inverted_index, batch_number):
    directory = 'batches_inverted_index'
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f'inverted_index_batch_{batch_number}.pkl.gz')
    with gzip.open(filename, 'wb') as f:
        pickle.dump(inverted_index, f)
    print(f"Saved inverted index for batch {batch_number} to {filename}")

def retrieve_inverted_index_doc(query, inverted_index, document_count):
    retrieved_documents = set()
    processed_query = processing_tokenize(query)

    for token in processed_query:
        if token in inverted_index:
            retrieved_documents.update(inverted_index[token])

    return {doc : document_count[doc] for doc in retrieved_documents if doc in document_count}
    