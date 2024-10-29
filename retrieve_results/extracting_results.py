
import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.vector_space_model import vsm_search_with_inverted_index_and_tfidf

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