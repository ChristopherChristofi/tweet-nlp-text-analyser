import time
import psycopg2
import re
import nltk
from nltk import sent_tokenize, wordpunct_tokenize
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

class Preprocessor:

    '''
    Responsible for normalization and provides essential text manipulation
    '''

    def __init__(self, data):
        self.data = data
        self.stopwords = set(stopwords.words('english'))
        self.lemmatizer = WordNetLemmatizer()

    def remove_mentions(self):
        for sentence in self.data:
            yield re.sub('@\w+', ' ', sentence)

    def remove_hyperlinks(self):
        for sentence in self.remove_mentions():
            yield re.sub(r'http\S+', ' ', sentence)

    def remove_short_words(self):
        for sentence in self.remove_hyperlinks():
            yield re.sub(r'\s+\w{2}\s+', ' ', sentence)

    def validate_alphabet(self):
        for sentence in self.remove_short_words():
            yield [token.lower() for token in nltk.word_tokenize(sentence) if token.isalpha()]

    def remove_stopwords(self):
        for sentence in self.validate_alphabet():
            yield [token for token in sentence if not token in self.stopwords]

    def lemmatize(self):
        for sentence in self.remove_stopwords():
            for token in sentence:
                yield self.lemmatizer.lemmatize(token)

    def normalize(self):
        return self.lemmatize()


class DataReader:

    '''
    Responsible for wrangling and reading a stream of text data extracted from database
    '''

    def __init__(self, uri):

        '''
        Instantiates the database connection
        '''

        self.cursor = psycopg2.connect(uri).cursor()

    def tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT tweet_text FROM raw_tweet_table;")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def tweet_ids(self):

        '''
        Responsible for generating tweet_id corpus from database.
        '''

        self.cursor.execute("SELECT tweet_id FROM raw_tweet_table;")
        for tweetid in iter(self.cursor.fetchone, None):
            yield tweetid

    def sentence_segmentation(self):

        '''
        Separates generated tweet text into sentence phrase segments.
        '''

        for tweet in self.tweets():
            for sentence in tweet:
                yield sent_tokenize(sentence)

    def word_tokenize(self):

        '''
        Responsible for generating tokens from the tweet text stream.
        '''

        for sentence in self.sentence_segmentation():
            for token in wordpunct_tokenize(str(sentence)):
                yield token

    def summary(self):

        '''
        Calculates simple analytical description of the data: counts and processing duration.
        '''

        init = time.time()

        counts = nltk.FreqDist()

        for tweet in self.sentence_segmentation():
            counts['tweets'] += 1

            for sentence in tweet:
                counts['sentences'] += 1

        return {
            'tweets': counts['tweets'],
            'sentences': counts['sentences'],
            'duration_secs': time.time() - init,
        }

    def transform(self):

        init = time.time()

        for sentence in self.sentence_segmentation():

            Preprocessor(sentence).normalize()

        duration = time.time() - init
        print(duration)
