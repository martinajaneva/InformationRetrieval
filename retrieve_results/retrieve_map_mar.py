
import pandas as pd
from helper.vector_space_model import vsm_search_with_inverted_index_and_tfidf
from helper.avg_funtions import avg_precision_at_k, avg_recall_at_k

"""
Determening MAP and MAR of large evaluation queries
"""
def find_map_mar(k_values, result, docs, results):
    for k in k_values:
        map_k = avg_precision_at_k(result, docs, k)
        map = "map_" + str(k) 
        results.loc[:, map] = round(map_k, 3)
        print(f"MAP@{k}: {map_k:.3f}")

        mar_k = avg_recall_at_k(result, docs, k) 
        mar = "mar_" + str(k)
        results.loc[:, mar] = round(mar_k, 3)
        print(f"MAR@{k}: {mar_k:.3f}")


def process_map_mar(query_docs, num_query, k, output, file_exists):
    for query, i in query_docs.items():
        detail = i['query']
        print(f"Processing {num_query} query - {detail}")
        result = vsm_search_with_inverted_index_and_tfidf(detail)
        docs = i['relevant_docs']

        print(f"Extracted query results {num_query}")
        num_query += 1

        results = pd.DataFrame(data = [{'num_query': query, 'text_query': detail}])
        
        find_map_mar(k, result, docs, results)
        results.to_csv(output, mode='a', index=False, header=not file_exists)
    
