import time, logging, psycopg2, re, unicodedata, nltk, arrow, csv
from nltk.sentiment import SentimentIntensityAnalyzer

class TweetProcessor:

    def __init__(self, data_id=None, tweet_date=None, data=None, filepath=None):
        self.data_id = data_id
        self.data = data
        self.tweet_date = tweet_date
        self.filepath = filepath
        self.analyzer = SentimentIntensityAnalyzer()

    def format_date (self):

        # format for raw tweet created_at date attribute:
        # "Mon Jan 01 23:59:59 +0000 2000"
        original_format = r"ddd[\s+]MMM[\s+]DD[\s+]HH:mm:ss[\s+]Z[\s+]YYYY"

        transformed_date = arrow.get(self.tweet_date, original_format).format('YYYY-MM-DD')

        transformed_time = arrow.get(self.tweet_date, original_format).format('HH:mm:ss')

        return transformed_date, transformed_time

    def format_data_generation(self, score):

        ''''
        Qualifies label based on the score for negative polarity, and builds the finalised
        data row for writing to the CSV file
        '''

        if score >= 0 and score <= 0.39: label = 0
        if score >= 0.4 and score <= 0.79: label = 1
        if score >= 0.8 and score <= 1: label = 2

        tweet_date, tweet_time = self.format_date()

        return [self.data_id, self.data, tweet_date, tweet_time, score, label]

    def transform(self, sentiment_score=None):

        '''
        Writes sentiment evaluated tweet data row to opened CSV file.
        '''

        row = []

        row.append(self.format_data_generation(sentiment_score))

        with open(file=self.filepath, mode="a", encoding="utf-8", newline="",) as f:
            write = csv.writer(f)
            write.writerows(row)

        logging.info("Tweet {id} saved to file: {filename}".format(id=row[0][0], filename=self.filepath[20:]))

    def generate_sentiment(self, sentence=None):

        '''
        Responsible for generating emotive polarity scores, returns only negative polarity.
        '''

        vs = self.analyzer.polarity_scores(''.join(sentence))
        return vs['neg']

    def remove_mentions(self, datum=None):

        return re.sub('@\w+', '', datum)

    def remove_hyperlinks(self, datum=None):

        return re.sub(r'http\S+', '', self.remove_mentions(datum))

    def normalize(self, sentence=None):

        '''
        Responsible for removing the presence of tokens of which begin with unicode category P(unctuation)
        '''

        validate_punctuation = lambda word: all(unicodedata.category(char).startswith('P') for char in word)
        sentence = filter(lambda token: not validate_punctuation(token[0]), sentence)
        return list(sentence)

    def extract_sentiment(self):

        '''
        Responsible for integrating text normalization methods, and calling the sentiment analyzer method before saving
        the relevent max negative polarity for a given tweet.
        '''

        sentiment_scores = []

        # initiate text normalization for each sentence of a tweet
        for sentence in nltk.sent_tokenize(self.remove_hyperlinks(self.data)):
            sentence = self.normalize(sentence)
            if not sentence: continue
            # build array of each relevant negative polarity score for a given tweet
            sentiment_scores.append(self.generate_sentiment(sentence))

        sentiment_score = max(sentiment_scores)

        logging.info("Sentiment calculated for tweet: {id}".format(id=self.data_id))

        # save the negative polarity and write to a new CSV output file
        self.transform(round(sentiment_score, 2))

class DataReader:

    '''
    Responsible for wrangling and reading a stream of text data extracted from database
    '''

    def __init__(self, uri=None):

        '''
        Instantiates the database connection
        '''

        self.init = time.time()
        self.filepath = "./data/stage/output/sentiment_data_{timestamp}.csv".format(timestamp=int(self.init))
        self.cursor = psycopg2.connect(uri).cursor()
        self.file_header = ['tweet_id', 'tweet_text', 'tweet_date', 'tweet_time', 'negative_polarity', 'label']

    def tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT tweet_id, date_created, tweet_text FROM raw_tweet_table LIMIT 100;")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def transform(self):

        '''
        Iniaties tweet data generator method and integrates tweet processor class transformation mechanisms
        '''

        # create new output file
        with open(file=self.filepath, mode="w", newline="") as f:
            write = csv.writer(f)
            write.writerow(self.file_header)

        logging.info("Data file created: {filename}".format(filename=self.filepath[20:]))

        # Process generated data objects from database
        for tweet in self.tweets():
            TweetProcessor(
                data_id=tweet[0],
                tweet_date=tweet[1],
                data=tweet[2],
                filepath=self.filepath
                ).extract_sentiment()

        duration = time.time() - self.init

        print("Sentiment analysis data streamer complete.")

        logging.info("Sentiment analysis streamreading complete. Duration: {time}s".format(time=int(duration)))