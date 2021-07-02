import os
from dotenv import load_dotenv

load_dotenv()

dbname=os.getenv("DATABASE") or "set name for database"
dbuser=os.getenv("RAW_USER") or "set user for database"
dbpassw=os.getenv("RAW_PASSW") or "set password for database"
dbhost=os.getenv("RAW_HOST") or "localhost"
dbport=os.getenv("RAW_PORT") or "5432"
data_store = "postgresql://{user}:{passw}@{host}:{port}/{database}".format(
    user=dbuser
    ,passw=dbpassw
    ,host=dbhost
    ,port=dbport
    ,database=dbname
)