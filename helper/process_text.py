
import re
import nltk

"""
Tokenizes and processes text.
Includes extension 3: unknown character removal, lemmatization, stopword removal & filtering of words.
Reference NLTK documentation & code adapted from https://spotintelligence.com/2022/12/21/nltk-preprocessing-pipeline/#14_Normalization
Args:
    text (str): The input text to be processed.
Returns:
    list: A list of filtered tokens after processing.
"""
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

def processing_tokenize(text):
    processed_text = tokenize(text)
    lemmatizer = nltk.WordNetLemmatizer()
    stopwords = nltk.corpus.stopwords.words("english")

    filter_words = [token for token in processed_text if token not in stopwords]
    lemmas = [lemmatizer.lemmatize(token) for token in filter_words]
    #stopwords = nltk.corpus.stopwords.words("english")
    filtered_tokens = [word for word in lemmas if word not in stopwords]
    return filtered_tokens