

import gzip
import os
import pickle
import string
import pandas as pd

"""
Merge class processes inverted index files and distributes the terms into separate Parquet files based on their starting letter.
Functions:
    loop_files(data_letter):
        Iterates over files in the current directory, processes those that match the naming pattern,
        and appends the terms and postings to the corresponding letter in the data_letter dictionary.
    inverted_index_distribution():
        Initializes the data_letter dictionary, calls loop_files to populate it, and then writes the
        data for each letter to a separate Parquet file.
"""

def loop_files(data_letter):
    for filename in os.listdir(''):
        print(f"Processing {filename}")
        if filename.startswith('inverted_index_batch_') and filename.endswith('.pkl.gz'):
            with gzip.open(filename, 'rb') as file:
                inverted_index = pickle.load(file)
                for i, post in inverted_index.items():
                    char = i[0].lower()
                    if char in string.ascii_lowercase:
                        data_letter[char].append({'term': i, 'postings': post})
                    else:
                        data_letter['other'].append({'term': i, 'postings': post})


def inverted_index_distribution():
    data_letter = {letter: [] for letter in string.ascii_lowercase}
    data_letter['other'] = []

    loop_files(data_letter)

    for letter, data in data_letter.items():
        df = pd.DataFrame(data)
        df.to_parqurt(f'inverted_index_{letter}.parquet', index=False)


inverted_index_distribution()