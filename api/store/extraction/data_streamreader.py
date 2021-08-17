import time, psycopg2, unicodedata, nltk, arrow, csv
from nltk.sentiment import SentimentIntensityAnalyzer

class TweetPreprocessor:

    def __init__(self, data_id=None, date=None, data=None, filepath=None):
        self.data_id = data_id
        self.data = data
        self.date = date
        self.analyzer = SentimentIntensityAnalyzer()
        self.filepath = filepath

    def format_date (self):

        # format for raw tweet created_at date attribute:
        # "Mon Jan 01 23:59:59 +0000 2000"
        original_format = r"ddd[\s+]MMM[\s+]DD[\s+]HH:mm:ss[\s+]Z[\s+]YYYY"

        transformed_date = arrow.get(self.date, original_format).format('YYYY-MM-DD')

        transformed_time = arrow.get(self.date, original_format).format('HH:mm:ss')

        return transformed_date, transformed_time

    def format_data_generation(self, score):

        if score >= 0 and score <= 0.39: state = 0
        if score >= 0.4 and score <= 0.79: state = 1
        if score >= 0.8 and score <= 1: state = 2

        tweet_date, tweet_time = self.format_date()

        return [self.data_id, self.data, tweet_date, tweet_time, score, state]

    def transform(self, sentiment_score=None):

        row = []

        row.append(self.format_data_generation(sentiment_score))

        with open(file=self.filepath, mode="a", encoding="utf-8", newline="",) as f:
            write = csv.writer(f)
            write.writerows(row)

    def generate_sentiment(self, sentence=None):

        vs = self.analyzer.polarity_scores(''.join(sentence))
        #print("{id}::{sent} - {sentiment}".format(id=self.data_id, sent=sentence, sentiment=str(vs)))
        return vs['neg']

    def normalize(self, sentence=None):

        '''
        Responsible for removing the presence of tokens of which begin with unicode category P(unctuation)
        '''

        validate_punctuation = lambda word: all(unicodedata.category(char).startswith('P') for char in word)
        sentence = filter(lambda token: not validate_punctuation(token[0]), sentence)
        return list(sentence)

    def extract_sentiment(self):

        sentiment_scores = []

        for sentence in nltk.sent_tokenize(self.data):
            sentence = self.normalize(self.data)
            if not sentence: continue
            sentiment_scores.append(self.generate_sentiment(sentence))

        sentiment_score = max(sentiment_scores)

        self.transform(round(sentiment_score, 2))

class DataReader:

    '''
    Responsible for wrangling and reading a stream of text data extracted from database
    '''

    def __init__(self, uri=None):

        '''
        Instantiates the database connection
        '''

        self.cursor = psycopg2.connect(uri).cursor()
        self.filepath = "./sentiment_data.csv"
        self.file_header = ['tweet_id', 'tweet_text', 'date', 'time', 'negative_polarity', 'label']

    def tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT tweet_id, date_created, tweet_text FROM raw_tweet_table;")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def transform(self):

        init = time.time()

        with open(file=self.filepath, mode="w", newline="") as f:
            write = csv.writer(f)
            write.writerow(self.file_header)

        for tweet in self.tweets():
            TweetPreprocessor(tweet[0], tweet[1], tweet[2], self.filepath).extract_sentiment()

        duration = time.time() - init
        print("\n\n\n", duration)
