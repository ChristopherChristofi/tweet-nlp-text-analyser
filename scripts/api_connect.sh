#!/usr/bin/bash

declare -A source_files
declare -A process_files

# Processing duration calculate
function timestampGen() {
    duration=$[ $(date +'%s') - $1 ]
    echo "Completion time: ${duration}"
}

function deselectRetweets() {
    # Function to deselect tweets where retweeted status is true with jq
    cat $1 | jq -c ' . | select(.retweeted_status == null)' > $2
}

function searchAPI() {
    # Responsible for connecting and searching the historic twitter API using twarc with defined parameters
    twarc search "'$1 OR $2 OR $3'" --lang en > $4
}

# Initial processing start time
init=$(date +'%s')

# Pre-selected Twitter API search terms
tweets_1=('insomnia' 'headache' 'stress')
tweets_2=('cry' 'anxiety' 'sad')
tweets_3=('depression' 'suicide' 'hurt')

# Filepaths to datasets - in case of error due operating system, access bash scripts directly
# and change filepaths to "../data/" instead of "./data/"
# Designated data extraction filepath
source_files[tweets_1]="./data/raw/raw_tweets_1_${init}.jsonl"
source_files[tweets_2]="./data/raw/raw_tweets_2_${init}.jsonl"
source_files[tweets_3]="./data/raw/raw_tweets_3_${init}.jsonl"

# Designated filepath retweet removal preprocessing filepath
process_files[tweets_1]="./data/raw/no_retweets_1_${init}.jsonl"
process_files[tweets_2]="./data/raw/no_retweets_2_${init}.jsonl"
process_files[tweets_3]="./data/raw/no_retweets_3_${init}.jsonl"

echo -e "\nSearch Twitter API:\n[1] 'insomnia' 'headache' 'stress'\n[2] 'cry' 'anxiety' 'sad'\n[3] 'depression' 'suicide' 'hurt'"
echo -ne "\nSelect option: "
# Number input option pattern
read OPTION

case $OPTION in

    1)
    # Set of functions responsible for connecting to the historic twitter API and basic retweet removal
    echo "Searching twitter API for '${OPTION}' selection. Initial time: ${init}"
    searchAPI "${tweets_1[0]}" "${tweets_1[1]}" "${tweets_1[2]}" "${source_files[tweets_1]}" \
    && deselectRetweets "${source_files[tweets_1]}" "${process_files[tweets_1]}"
    ;;

    2)
    echo "Searching twitter API for '${OPTION}' option selection. Initial time: ${init}"
    searchAPI "${tweets_2[0]}" "${tweets_2[1]}" "${tweets_2[2]}" "${source_files[tweets_2]}" \
    && deselectRetweets "${source_files[tweets_2]}" "${process_files[tweets_2]}"
    ;;

    3)
    echo "Searching twitter API for '${OPTION}' option selection. Initial time: ${init}"
    searchAPI "${tweets_3[0]}" "${tweets_3[1]}" "${tweets_3[2]}" "${source_files[tweets_3]}" \
    && deselectRetweets "${source_files[tweets_3]}" "${process_files[tweets_3]}"
    ;;

    *)
    echo "Incorrect option selection provided."
    ;;

esac;
timestampGen "${init}"