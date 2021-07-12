from dask import dataframe as dd
from data.resources import data_store
from data.raw.resources import dbtweettable, dbhashtagtable

tweet_filepath = "./data/raw/output/tweet_data*.csv"

hashtag_filepath = "./data/raw/output/hashtag_data*.csv"

def load_data(path=None, dbtable=None, headers=None):

    """
    Called function that writes loaded data into database.
    """

    DF = dd.read_csv(
        urlpath=path,
        names=headers)

    dd.to_sql(
        DF,
        name=dbtable,
        uri=data_store,
        if_exists='append',
        index=False
        )

def integrate_load(tweets=0, hashtags=0):

    """
    Parent function based on user input that integrates loading of processed data, using
    Dask and pandas dataframe functionality, into SQL database using SQLAlchemy expressions.

    """

    if tweets == True:

        print("Loading Data.")

        load_data(
            path=tweet_filepath,
            dbtable=dbtweettable,
            headers=[
                'tweet_id',
                'user_id',
                'date_created',
                'tweet_text'
                ]
            )
        print("Tweets loaded.")

    if hashtags == True:

        print("Loading data")

        load_data(
            path=hashtag_filepath,
            dbtable=dbhashtagtable,
            headers=[
                'hashtag',
                'tweet_id'
                ]
            )
        print("Hashtags loaded.")