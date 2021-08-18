from dask import dataframe as dd
from data.resources import data_store
from data.stage.resources import dbsentimenttable

tweet_filepath = "./data/stage/output/sentiment_data_*.csv"

def load_data(path=None, dbtable=None, headers=None):

    """
    Called function that writes loaded data into database.
    """

    DF = dd.read_csv(
        urlpath=path,
        names=headers,
        dtype='unicode')

    dd.to_sql(
        DF,
        name=dbtable,
        uri=data_store,
        if_exists='append',
        index=False
        )

def integrate_load(sentiment_tweets=0):

    """
    Parent function based on user input that integrates loading of sentiment data, using
    Dask and pandas dataframe functionality, into SQL database using SQLAlchemy expressions.

    """

    if sentiment_tweets == True:

        print("Loading Data.")

        load_data(
            path=tweet_filepath,
            dbtable=dbsentimenttable,
            headers=[
                'tweet_id',
                'tweet_text',
                'tweet_date',
                'tweet_time',
                'sentiment_value',
                'label_polarity'
                ]
            )
        print("Sentiment value scores loaded.")