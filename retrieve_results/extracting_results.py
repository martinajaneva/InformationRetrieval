
import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.vector_space_model import vsm_search_with_inverted_index_and_tfidf

"""
Process a set of queries and save the top 10 results for each query to a CSV file.
Arguments:
    query_docs (dict): A dictionary where keys are query identifiers and values are dictionaries containing the query details.
    num_query (int): A counter for the number of queries processed, used for logging.
    header_written (bool): A flag indicating whether the CSV header has been written.
Returns:
    None: The function appends results to a CSV file and does not return a value.
"""
def extract_results(query_docs, num_query, header_written):
    for query, i in query_docs.items():
        detail = i['query']
        print(f"Processing {num_query} query - {detail}")
        num_query += 1
        result = vsm_search_with_inverted_index_and_tfidf(detail)
        documents = [term[0] for term in result[:10]]

        results = pd.DataFrame({'Query Number': [query] * len(documents),
                                'Document Number': documents})
        # print(results)
        results.to_csv('queries/results.csv', mode='a', index=False, header=header_written)
        header_written = False