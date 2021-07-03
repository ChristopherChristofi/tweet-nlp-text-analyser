import subprocess
from api_connect.store.data_options import load_data_options
from api_connect.store.extraction.data_streamreader import DataReader
from data.resources import data_store

options = {
    "api_option" : "[1] - Search Twitter API (Overwrites; Removes gathered retweets; Commandline Method(CM))",
    "format_option" : "[2] - Format Raw Data (CM)",
    "load_raw_data" : "[3] - Load Raw Data",
    "reprint_options" : "\n[101] - Reprint all options",
    "exit_option" : "[0] - Exit program"
}

def print_options():

    print("\n")
    for v in options.values():
        print(v)

if __name__ == "__main__":

    print_options()

    start = 1

    while start == True:
        option = int(input('\nSelect an option > '))
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
        if option == 0:
            print("Exit")
            start = False

        #test datareader
        if option == 5:
            DataReader(data_store).transform()
