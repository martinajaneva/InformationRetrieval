from collections import defaultdict
from multiprocessing import cpu_count, Pool
import os
from pathlib import Path
import time
from helper.inverted_index import inverted_index_build, save_inverted_index, save_term_counter

"""
Extracting inverted index class.
Multiprocessing is used to speed up the process.
References include documentation of multiprocessing in Python.
https://docs.python.org/3/library/multiprocessing.html
"""

def process_batches(files_total, batch_size, text_files, processes_val, inverted_index, term_counter):
    for i in range(0, files_total, batch_size):
        batch = text_files[i:i+batch_size]
        print(f"Processing batch {batch // batch_size + 1}")
        with Pool(processes=processes_val) as pool:
            res = pool.map(inverted_index_build, batch)

        for id, count in res:
            for token, files in id.items():
                inverted_index[token].extend(files)
            term_counter.update(count)

        num_batch = batch // batch_size + 1

        save_inverted_index(inverted_index=inverted_index, batch_number=num_batch)
        print(f"Inverted index saved batch {num_batch}")
        inverted_index.clear()
        
        save_term_counter(term_counter=term_counter, batch_number=num_batch)
        print(f"Document term saved batch {num_batch}")
        term_counter.clear()

def multiprocessing(directory_path, batch_size = 20000):
    path = Path(directory_path).rglob('*.txt')
    text_files = list(path)
    files_total = len(text_files)

    inverted_index = defaultdict(list)
    term_counter = {}

    processes_val = cpu_count()

    start_time = time.time()
    process_batches(files_total, batch_size, text_files, processes_val, inverted_index, term_counter)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds for {files_total} files")

if __name__ == "__main__":
    base_path = os.getcwd()

    # This can be adapted with the path to the data
    data_path = "full_docs"
    full_path = os.path.join(base_path, data_path)
    print(full_path)

    multiprocessing(full_path)