from process_text import processing_tokenize
from inverted_index import retrieve_inverted_index_doc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

"""
Preprocesses document term counts for TF-IDF vectorization.
This step prepares the documents by extracting term keys (words) from their term frequency data and concatenating them into a single string.
Args:
    doc_term_counts (dict): A mapping of document IDs to term frequency counts.
Returns:
    dict: A dictionary mapping document IDs to their preprocessed text content as strings.
"""
def preprocess_documents(doc_term_counts):
    preprocessed_texts = {}
    
    for doc, term_count in doc_term_counts.items():
        preprocessed_texts[doc] = ' '.join(term_count.keys())
    return preprocessed_texts

"""
Preprocesses the search query for TF-IDF vectorization.
This function tokenizes the query text and prepares it for vectorization using TF-IDF.
Args:
    search_query (str): The input search query.
Returns:
    str: The preprocessed query as a single string of tokens.
"""
def preprocess_query(search_query):
    preprocessed_query = ' '.join(processing_tokenize(search_query))
    return preprocessed_query

"""
Performs a document search using a Vector Space Model (VSM) with an inverted index and TF-IDF vectorization.
This function integrates the following key components: Inverted Index Retrieval, TF-IDF Vectorization and Cosine Similarity Calculation
References:
- Code adapted from: https://www.restack.io/p/information-retrieval-knowledge-vector-space-model-cat-ai

Args:
    search_query (str): The input search query.
    inverted_index (dict): The inverted index mapping terms to document IDs.
    doc_term_counts (dict): A mapping of document IDs to term frequency counts.

Returns:
    list: A sorted list of tuples containing document names and their corresponding similarity scores.
"""
def vsm_search_with_inverted_index_and_tfidf(search_query, inverted_index, doc_term_counts):
    matched_docs = retrieve_inverted_index_doc(search_query, inverted_index, doc_term_counts)

    if not matched_docs:
        return []

    preprocessed_docs = preprocess_documents(doc_term_counts)
    preprocessed_query = preprocess_query(search_query)

    # doc_names = list(relevant_docs.keys())
    # doc_texts = [' '.join(term_count.keys()) for term_count in relevant_docs.values()]
    doc_names = list(preprocessed_docs.keys())
    doc_texts = list(preprocessed_docs.values())

    tfidf_vectorizer = TfidfVectorizer(tokenizer=processing_tokenize)
    tfidf_matrix = tfidf_vectorizer.fit_transform(doc_texts)
    query_vector = tfidf_vectorizer.transform([preprocessed_query])
    similarity_scores = cosine_similarity(query_vector, tfidf_matrix).flatten()

    ranked_documents = sorted(zip(doc_names, similarity_scores), key=lambda x: x[1], reverse=True)

    return ranked_documents