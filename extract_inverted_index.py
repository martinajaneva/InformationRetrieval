from collections import defaultdict
from multiprocessing import cpu_count, Pool
import os
from pathlib import Path
import time
from helper.inverted_index import inverted_index_build, save_inverted_index

"""
Extracting inverted index class.
Multiprocessing is used to speed up the process.
References include documentation of multiprocessing in Python.
https://docs.python.org/3/library/multiprocessing.html
"""

def process_batches(files_total, batch_size, text_files, processes_val, inverted_index):
    for i in range(0, files_total, batch_size):
        batch = text_files[i:i+batch_size]
        num_batch = i // batch_size + 1
        print(f"Processing batch {num_batch}")
        with Pool(processes=processes_val) as pool:
            res = pool.map(inverted_index_build, batch)

        for id in res:
            for token, files in id.items():
                inverted_index[token].extend(files)

        save_inverted_index(inverted_index=inverted_index, batch_number=num_batch)
        print(f"Inverted index saved batch {num_batch}")
        inverted_index.clear()

def multiprocessing(directory_path, batch_size = 300):
    path = Path(directory_path).rglob('*.txt')
    text_files = list(path)
    files_total = len(text_files)

    inverted_index = defaultdict(list)

    processes_val = cpu_count()

    start_time = time.time()
    process_batches(files_total, batch_size, text_files, processes_val, inverted_index)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds for {files_total} files")

if __name__ == "__main__":
    base_path = os.getcwd()

    # This can be adapted with the path to the data
    data_path = "Data/full_docs_small"
    full_path = os.path.join(base_path, data_path)
    print(full_path)

    multiprocessing(full_path)