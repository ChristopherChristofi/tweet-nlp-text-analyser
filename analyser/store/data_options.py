# same root database configuration
from api.store.configuration import build
from analyser.store.load_data import integrate_load

options = {
    "tweets_option" : "[1] - Load Sentiment Data",
    "exit_option" : "[0] - Exit options"
}

def print_options():

    """
    Prints user selection options
    """

    print("\n")
    for v in options.values():
        print(v)

def load_stage_options(run=0):

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
            integrate_load(sentiment_tweets=1)
        if option == 0:
            print("Exit")
            start = False