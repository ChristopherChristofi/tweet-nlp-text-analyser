import os
from dotenv import load_dotenv

load_dotenv()

dbname=os.getenv("DATABASE") or "set name for database"
dbuser=os.getenv("RAW_USER") or "set user for database"
dbpassw=os.getenv("RAW_PASSW") or "set password for database"
data_store = "postgresql://{user}:{passw}@localhost:5432/{database}".format(
    user=dbuser
    ,passw=dbpassw
    ,database=dbname
)