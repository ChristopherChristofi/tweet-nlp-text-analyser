import logging, psycopg2, csv, time

class SentimentDataReader:

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

        logging.info("Data file created: {filepath}".format(filepath=filepath[22:]))

    def generate_csv_data(self, filepath=None, data=None):

        with open(file=filepath, mode="a", encoding="utf-8", newline="") as f:
            write = csv.writer(f)
            write.writerows(data)

        logging.info("Tweet {id} saved to file: {filepath}".format(id=data[0][0], filepath=filepath[22:]))

    def generate_tweets(self, polarity=None):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT * FROM stage_sentiment_table WHERE label_polarity='{polarity}';".format(polarity=polarity))
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def generate_label_data(self, run=0, polarity=None):

        logging.info("Generating Sentiment labelled dataset by polarity: {polarity}".format(polarity=polarity))

        if run:

            row = []

            label = "label_{polarity}".format(polarity=str(polarity))

            filepath = './data/stage/analysis/{file_label}_data_{time}.csv'.format(file_label=label, time=int(self.init))

            if polarity or polarity == 0:

                self.build_data_file(filepath=filepath)

                for tweet in self.generate_tweets(polarity=polarity):
                    row.append(tweet)
                    self.generate_csv_data(filepath=filepath, data=row)
                    row = []

        duration = time.time() - self.init

        print("Labelled sentiment data by polarity: {polarity} generated.".format(polarity=polarity))

        logging.info("Labelled sentiment data by polarity: {polarity} generated. Duration: {time}s".format(polarity=polarity, time=int(duration)))
