import logging, psycopg2, csv, time

class DataReader:

    '''
    Responsible for wrangling and reading a stream of text data extracted from database
    '''

    def __init__(self, uri=None):

        '''
        Instantiates the database connection
        '''
        self.cursor = psycopg2.connect(uri).cursor()
        self.init = time.time()
        self.file_header = ['tweet_id', 'tweet_text', 'tweet_date', 'tweet_time', 'negative_polarity', 'label']

    def build_data_file(self, filepath=None):

        with open(file=filepath, mode="w", newline="") as f:
            write = csv.writer(f)
            write.writerow(self.file_header)

    def generate_csv_data(self, filepath=None, data=None):

        self.build_data_file(filepath=filepath)

        with open(file=filepath, mode="a", encoding="utf-8", newline="") as f:
            write = csv.writer(f)
            write.writerows(data)

    def extreme_negative_tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT * FROM stage_sentiment_table WHERE label_polarity='2';")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def negative_tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT * FROM stage_sentiment_table WHERE label_polarity='1';")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def not_negative_tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT * FROM stage_sentiment_table WHERE label_polarity='0';")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def generate_label_data(self, run=0, polarity=None):

        if run:

            row = []

            label = "label_{polarity}".format(polarity=str(polarity))

            filepath = './data/stage/analysis/{file_label}_data_{time}.csv'.format(file_label=label, time=self.init)

            if polarity == 0:

                for tweet in self.not_negative_tweets():
                    row.append(tweet)
                    self.generate_csv_data(filepath=filepath, data=row)

            if polarity == 1:

                for tweet in self.negative_tweets():
                    row.append(tweet)
                    self.generate_csv_data(filepath=filepath, data=row)

            if polarity == 2:

                for tweet in self.extreme_negative_tweets():
                    row.append(tweet)
                    self.generate_csv_data(filepath=filepath, data=row)
