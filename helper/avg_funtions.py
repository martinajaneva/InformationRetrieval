"""
Calculate the Average Precision at a specified rank (AP@K) for a given query
Args:
    results (list): A list of tuples (document, similarity score), sorted by highest to lowest similarity.
    relevant_set (set): A set of documents relevant to the query.
    k (int): Rank position up to which precision is computed.
Returns:
    float: Average Precision at the specified K.
"""
def avg_precision_at_k(results, relevant_set, k):
    precision_sum = 0.0
    count = 0
    
    for pos, (doc, _) in enumerate(results[:k], 1):
        if doc in relevant_set:
            count += 1
            precision_sum += count / pos
     
    return precision_sum / count if count > 0 else 0.0

"""
Calculate the Average Recall at a specified rank (AR@K) for a given query.
Arguments:
    results (list): A list of tuples (document, similarity score), sorted by highest to lowest similarity.
    relevant_set (set): A set of documents relevant to the query.
    k (int): Rank position up to which recall is calculated.
Returns:
    float: Average Recall at the specified K.
"""
def avg_recall_at_k(results, relevant_set, k):
    count = 0
    total_relevant = len(relevant_set)
    
    for doc, _ in results[:k]:
        if doc in relevant_set:
            count += 1
    
    return count / total_relevant if total_relevant > 0 else 0.0