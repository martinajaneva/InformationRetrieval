

import gzip
import os
import pickle
import string
import pandas as pd

"""
Merge class processes inverted index files and distributes the terms into separate Parquet files based on their starting letter.
References: https://codetinkering.com/combining-dataframes-python/
Functions:
    loop_files(data_letter):
        Iterates over files in the current directory, processes those that match the naming pattern,
        and appends the terms and postings to the corresponding letter in the data_letter dictionary.
    inverted_index_distribution():
        Initializes the data_letter dictionary, calls loop_files to populate it, and then writes the
        data for each letter to a separate Parquet file.
"""

def loop_files(data_letter):
    for filename in os.listdir('batches_inverted_index'):
        full_path = os.path.join('batches_inverted_index', filename)
        print(f"Processing {full_path}")
        if filename.startswith('inverted_index_batch_') and filename.endswith('.pkl.gz'):
            with gzip.open(full_path, 'rb') as file:
                inverted_index = pickle.load(file)
                for i, post in inverted_index.items():
                    char = i[0].lower()
                    if char in string.ascii_lowercase:
                        data_letter[char].append({'term': i, 'postings': post})
                    else:
                        data_letter['other'].append({'term': i, 'postings': post})

def save_files(data_letter):
    directory = 'letter_files'
    if not os.path.exists(directory):
        os.makedirs(directory)
    for letter, data in data_letter.items():
        df = pd.DataFrame(data)
        file_path = os.path.join(directory, f'inverted_index_{letter}.parquet')
        df.to_parquet(file_path, index=False)

def inverted_index_distribution():
    data_letter = {letter: [] for letter in string.ascii_lowercase}
    data_letter['other'] = []

    loop_files(data_letter)
    save_files(data_letter)

inverted_index_distribution()