from sqlalchemy import create_engine, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database, database_exists
from data.resources import data_store
from data.raw.resources import dbusertable, dbtweettable, dbhashtagtable

Base = declarative_base()

# Raw database Schema

class TwitterTweet(Base):
    __tablename__ = dbtweettable

    id = Column(Integer, primary_key=True)
    tweet_id = Column(String, unique=True)
    date_created = Column(String)
    tweet_text = Column(String)

    def __init__(self, tweet_id, date_created, tweet_text):
        self.tweet_id = tweet_id
        self.date_created = date_created
        self.tweet_text = tweet_text

class TwitterUser(Base):
    __tablename__  = dbusertable

    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    tweet_id = Column(String, ForeignKey("{table}.tweet_id".format(table=dbtweettable)))
    tweet = relationship("TwitterTweet")

    def __init__(self, user_id):
        self.user_id = user_id

class TwitterHashtag(Base):
    __tablename__ = dbhashtagtable

    id = Column(Integer, primary_key=True)
    hashtag = Column(String)
    tweet_id = Column(String, ForeignKey("{table}.tweet_id".format(table=dbtweettable)))
    tweet = relationship("TwitterTweet")

    def __init__(self, hashtag):
        self.hashtag = hashtag

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

        print("Connection successful")