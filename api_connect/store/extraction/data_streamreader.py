import time
import psycopg2
from data.resources import data_store
import nltk
from nltk import sent_tokenize, wordpunct_tokenize

class DataReader:

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

def show_it(run=0):

    '''
    Test init function connected to main.py
    '''

    if run == True:

        register = DataReader(data_store)

        print(register.summary())