import subprocess, logging
from archive import logger
from api.store.data_options import load_data_options
from analyser.extraction.streamreader import DataReader
from analyser.store.data_options import load_stage_options
from analyser.store.analytical_options import load_analytic_options
from data.resources import data_store

options = {
    "api_option" : "[1] - Search Twitter API (Overwrites; Removes gathered retweets; Commandline Method(CM))",
    "format_option" : "[2] - Format Raw Data (CM)",
    "load_raw_data" : "[3] - Load Raw Data",
    "sentiment_option" : "[4] - Initiate preprocessing and generate sentiment analysis",
    "load_stage_data" : "[5] - Load Stage Data into database (Sentiment Score Data)",
    "load_analytical_data" : "[6] - Generate Final Analytical Data (Labelled Sentiment Score Data)",
    "reprint_options" : "\n[101] - Reprint all options",
    "exit_option" : "[0] - Exit program"
}

def print_options():

    print("\n")
    for v in options.values():
        print(v)

if __name__ == "__main__":

    print_options()

    logger("projectlogfile.log")

    start = 1

    logging.info("Project started")

    while start == True:
        option = int(input('\nSelect an option > '))
        logging.info("Option selected: {selection}".format(selection=option))
        if option == 101:
            print_options()
        if option == 1:
            subprocess.run(
                './scripts/api_connect.sh'
                )
        if option == 2:
            subprocess.run(
                './scripts/raw_data_formatting.sh'
                )
        if option == 3:
            load_data_options(run=1)
        if option == 4:
            DataReader(data_store).transform()
        if option == 5:
            load_stage_options(run=1)
        if option == 6:
            load_analytic_options(run=1)
        if option == 0:
            print("Exit")
            start = False