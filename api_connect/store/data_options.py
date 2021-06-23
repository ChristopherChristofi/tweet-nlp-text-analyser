from api_connect.store.configuration import build
from api_connect.store.load_json import integrate_load

options = {
    "tweets_option" : "[1] - Load Tweet Data",
    "hashtags_option" : "[2] - Load Hashtag Data",
    "reprint_options" : "\n[101] - Reprint all options",
    "exit_option" : "[0] - Exit program"
}

def print_options():

    """
    Prints user selection options
    """

    print("\n")
    for v in options.values():
        print(v)

def load_data_options(run=0):

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
            integrate_load(tweets=1, hashtags=0)
        if option == 2:
            integrate_load(tweets=0, hashtags=1)
        if option == 0:
            print("Exit")
            start = False