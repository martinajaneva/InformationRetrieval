
import os
import pandas as pd
from retrieve_results.extracting_results import extract_results
from retrieve_results.retrieve_map_mar import process_map_mar


def extract_docs_queries(first_row, batch_size, retrieve_docs = False):
    queries_relevant_text = {}
    batch =  queries.iloc[first_row: first_row + batch_size]

    for _, j in batch.iterrows():
        id = j['Query number']
        query_text = j['Query']
        if(retrieve_docs == True):
            relevant_docs = queries_results[queries_results['Query_number'] == id]['doc_number'].astype(str).tolist()
            queries_relevant_text[id] = {
                'query' : query_text, 
                'relevant_docs': relevant_docs }
        else:
            queries_relevant_text[id] = {'query' : query_text}
        
    return queries_relevant_text


def initialize():
    output = 'queries/results.csv'
    file_exists = os.path.isfile(output)

    first_row = 0
    batch_size = 300
    k = [3, 10]
    header_written = not file_exists

    num_query = first_row + 1
    
    # Calculate MAP MAR values (Following 2 lines of code should be uncommented for calculation)
   
    # query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=True)
    # process_map_mar(query_docs, num_query, k, output, file_exists)

    #############################################################################################

    # Extract results of queries (Following 2 lines of code should be uncommented for calculation)

    # query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=False)
    # extract_results(query_docs, num_query, header_written)


queries = pd.read_csv('queries/chosen_queries.csv')
queries_results = pd.read_csv('queries/chosen_queries_results.csv')

initialize()




