# same root database configuration
from api.store.configuration import build
from analyser.analysis.streamreader import SentimentDataReader
from data.resources import data_store

options = {
    "not_negative_option" : "[1] - Generate tweet csv data labeled not negative",
    "negative_option" : "[2] - Generate tweet csv data labeled negative",
    "severe_negative_option" : "[3] - Generate tweet csv data labeled extreme negative",
    "exit_option" : "[0] - Exit options"
}

def print_options():

    """
    Prints user selection options
    """

    print("\n")
    for v in options.values():
        print(v)

def load_analytic_options(run=0):

    """
    Data loading parameters based on user input. Definitions found in options dict.
    """

    # Qualify database existence or build
    build(run)

    print_options()

    start = 1

    while start == True:
        # Parameter definitions in options dict
        option = int(input('\nSelect an option > '))
        if option == 101:
            print_options()
        if option == 1:
            print("Loading data...")
            SentimentDataReader(data_store).generate_label_data(run=1, polarity=0)
        if option == 2:
            print("Loading data...")
            SentimentDataReader(data_store).generate_label_data(run=1, polarity=1)
        if option == 3:
            print("Loading data...")
            SentimentDataReader(data_store).generate_label_data(run=1, polarity=2)
        if option == 0:
            print("Exit")
            start = False