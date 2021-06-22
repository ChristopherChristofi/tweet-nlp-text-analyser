import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

"""
    Not currently used in full program, displaced by Dask and pandas, as that is lighter-weight.
    Fully functional data loading features with PySpark and kept for future reference.
    Instances such as: tweet_data or user_data, are callable and can be used as functional
    dataframes that hold their relevant loaded dataset, which can be written to SQL database,
    methods found below.
"""

load_dotenv()

DBdriver = os.getenv("RAWDBDRIVER") or "Sort Postgresql Driver"

tweet_filepath = "./data/raw/output/*tweets.csv"

user_filepath = "./data/raw/output/*users.csv"

hashtag_filepath = "./data/raw/output/*hashtags.csv"

def load_data(spark, schema=None, path=None, dbname=None, query=None):

    DataFrame = spark.read.option("multiLine","true") \
        .option("delimeter", ",") \
        .format("csv") \
        .schema(schema) \
        .load(path)

    DataFrame.registerTempTable(dbname)

    output = spark.sql(query)
    # output.show()
    return output

raw_tweet_schema = StructType() \
    .add("tweet_id", StringType(), False) \
    .add("date_created", StringType(), False) \
    .add("tweet_text", StringType(), False)

raw_user_schema = StructType() \
    .add("user_id", StringType(), False) \
    .add("tweet_id", StringType(), False)

raw_hashtag_schema = StructType() \
    .add("hashtag", StringType(), False) \
    .add("tweet_id", StringType(), False)

scSpark = SparkSession \
    .builder \
    .appName("readRawData") \
    .config("spark.driver", DBdriver) \
    .getOrCreate()

tweet_data = load_data(
    spark=scSpark
    ,schema=raw_tweet_schema
    ,path=tweet_filepath
    ,dbname="tweets"
    ,query='SELECT tweet_id, date_created, tweet_text FROM tweets'
    )

user_data = load_data(
    spark=scSpark
    ,schema=raw_user_schema
    ,path=user_filepath
    ,dbname="users"
    ,query='SELECT user_id, tweet_id FROM users'
    )

hashtag_data = load_data(
    spark=scSpark
    ,schema=raw_hashtag_schema
    ,path=hashtag_filepath
    ,dbname="hashtags"
    ,query='SELECT hashtag, tweet_id FROM hashtags'
    )

# A complementary module that initiates writing to database. Appended here based on main comment above.
from data.resources import dbname, dbuser, dbpassw
from api_connect.store.configuration import build
from data.raw.resources import dbtweettable, dbusertable, dbhashtagtable
#from api_connect.store.play.grab_json_spark import user_data, tweet_data, hashtag_data

url = "jdbc:postgresql://localhost/{database}".format(
    database=dbname
)

def load_frame(tweets=0, users=0, hashtags=0):

    build(run=1)

    if tweets == True:

        tweet_data.write \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", dbtweettable) \
            .option("user", dbuser) \
            .option("password", dbpassw) \
            .mode("append") \
            .save()

    if users == True:

        user_data.write \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", dbusertable) \
            .option("user", dbuser) \
            .option("password", dbpassw) \
            .mode("append") \
            .save()

    if hashtags == True:

        hashtag_data.write \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", dbhashtagtable) \
            .option("user", dbuser) \
            .option("password", dbpassw) \
            .mode("append") \
            .save()