import warnings
from helper.process_text import processing_tokenize, tokenize
from helper.inverted_index import retrieve_inverted_index_doc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import concurrent.futures

warnings.filterwarnings("ignore", message="The parameter 'token_pattern' will not be used since 'tokenizer' is not None")

def load_document(document):
    with open(f"Data/full_docs/{document}", 'r', encoding='utf-8') as f:
        return f.read()

"""
Preprocesses document term counts for TF-IDF vectorization.
This step prepares the documents by extracting term keys (words) from their term frequency data and concatenating them into a single string.
References: https://scikit-learn.org/1.5/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
Args:
    doc_term_counts (dict): A mapping of document IDs to term frequency counts.
Returns:
    dict: A dictionary mapping document IDs to their preprocessed text content as strings.
"""
def preprocess_documents(doc_term_counts):
    preprocessed_texts = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        doc_texts = list(executor.map(load_document, doc_term_counts))
    
    for doc, term_count in zip(doc_term_counts, doc_texts):
        preprocessed_texts[doc] = term_count
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
def vsm_search_with_inverted_index_and_tfidf(search_query):
    matched_docs = retrieve_inverted_index_doc(search_query)
    if not matched_docs:
        return []
    
    preprocessed_query = preprocess_query(search_query)
    preprocessed_docs = preprocess_documents(matched_docs)
    doc_texts = list(preprocessed_docs.values())
    doc_names = list(preprocessed_docs.keys())

    tfidf_vectorizer = TfidfVectorizer(tokenizer=tokenize, max_features=2**14, ngram_range=(1, 2))
    tfidf_matrix = tfidf_vectorizer.fit_transform(doc_texts)
    query_vector = tfidf_vectorizer.transform([preprocessed_query])
    similarity_scores = cosine_similarity(query_vector, tfidf_matrix).flatten()

    names_cleaned_doc = [(doc.replace('output_', '').replace('.txt', '')) for doc in doc_names]
    ranked_documents = sorted(zip(names_cleaned_doc, similarity_scores), key=lambda x: x[1], reverse=True)

    return ranked_documents