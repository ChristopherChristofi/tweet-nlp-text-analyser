from sqlalchemy import create_engine, Column, Integer, String, Time, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database, database_exists
from data.resources import data_store
from data.stage.resources import dbsentimenttable

Base = declarative_base()

# Stage Sentiment Analysis database Schema

class TweetSentiment(Base):
    __tablename__ = dbsentimenttable

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String, unique=True)
    tweet_text = Column(String)
    tweet_date = Column(Date)
    tweet_time = Column(Time)
    sentiment_value = Column(Float)
    label_polarity = Column(Integer)

    def __init__(self, tweet_id, tweet_text, tweet_date, tweet_time, sentiment_value, label_polarity):
        self.tweet_id = tweet_id
        self.tweet_text = tweet_text
        self.tweet_date = tweet_date
        self.tweet_time = tweet_time
        self.sentiment_value = sentiment_value
        self.label_polarity = label_polarity


def build(run=0):

    """
    Inits with data loading. Using SQLAlchemy and Postgresql, depending of existence,
    creates the database or initiates the database engine for proposed data loading.
    """

    if run == True:

        print("Generating database engine..")

        engine = create_engine(data_store)
        if not database_exists(engine.url):
            create_database(engine.url)
            print("Database created.")

        Base.metadata.create_all(bind=engine)

        print("Connection successful to Stage Database - Sentiment Analysis")