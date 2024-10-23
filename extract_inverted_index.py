
import re
import nltk

"""
Tokenizes and processes text.
Includes extension 3: unknown character removal, lemmatization & stopword removal.
Reference NLTK documentation & code adapted from https://spotintelligence.com/2022/12/21/nltk-preprocessing-pipeline/#14_Normalization
Args:
    text (str): The input text to be processed.
Returns:
    list: A list of filtered tokens after processing.
"""
def processing_tokenize(text):
    processed_text = re.findall(r'\b\w+\b', text.lower())
    lemmatizer = nltk.WordNetLemmatizer()
    lemmas = [lemmatizer.lemmatize(word) for word in processed_text]
    stopwords = nltk.corpus.stopwords.words("english")
    filtered_tokens = [word for word in lemmas if word not in stopwords]
    return filtered_tokens


