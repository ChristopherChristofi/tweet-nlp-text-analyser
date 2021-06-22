#!/usr/bin/bash

declare -A source_files
declare -A conversion_files
declare -A process_files
declare -A combined_output
declare -A collect_files

# Designated data extraction filepath
source_files[file_1]="./data/raw/raw_no_retweets_1.jsonl"
source_files[file_2]="./data/raw/raw_no_retweets_2.jsonl"

# Designated conversion data file filepaths
conversion_files[user_file_1]="./data/raw/process/user_data_1.csv"
conversion_files[tweet_file_1]="./data/raw/process/tweet_data_1.csv"
conversion_files[hashtag_file_1]="./data/raw/process/hashtag_data_1.csv"
conversion_files[user_file_2]="./data/raw/process/user_data_2.csv"
conversion_files[tweet_file_2]="./data/raw/process/tweet_data_2.csv"
conversion_files[hashtag_file_2]="./data/raw/process/hashtag_data_2.csv"

# Designated processing file for tweet data; input and process manipulation
process_files[tweet_file_1]="./data/raw/process/tweet_process_1.jsonl"
process_files[tweet_file_2]="./data/raw/process/tweet_process_2.jsonl"

# Designated processing file for tweet data; input and process manipulation
collect_files[user_files]="./data/raw/process/user*.csv"
collect_files[tweet_files]="./data/raw/process/tweet*.csv"
collect_files[hashtag_files]="./data/raw/process/hashtag*.csv"

# Designated filepath for tweet data processing process
combined_output[users_file]="./data/raw/output/combined_users.csv"
combined_output[tweets_file]="./data/raw/output/combined_tweets.csv"
combined_output[hashtags_file]="./data/raw/output/combined_hashtags.csv"

# Processing duration calculate
function timestampGen() {
    duration=$[ $(date +'%s') - $1 ]
    echo "Completion time: ${duration}"
}

# Data jsonl conversion to csv with formatting by input parameters
function formatFile() { cat $1 | jq -cr ". | $2 | @csv" > $3 && echo "File created: $3."; }

# Data jsonl conversion to csv with formatting by input parameters.
# Replaces newline escape characters with tabs to prevent parsing errors
# and maintain csv delimitation and lining. eg tweet text overline spills over
# to newline, ends up becoming tweet id column when error causeing newline escape is present.
function formatTweetFile() {
    cat $1 | jq -c '. | {tweet_id: .id_str, date_created: .created_at, tweet_text: .full_text}' > $2 \
    && sed 's/\\n/\\t/g' $2 | jq -cr ". | [.tweet_id, .date_created, .tweet_text] | @csv" > $3 && echo "File created: $3.";
}

# Function to remove duplicate data gather after combination of datasets
function removeDuplicates() { sort $1 -u > $2 && echo "Merge complete. File created: $2"; }

# Initial processing start time
init=$(date +'%s')

echo -e "\nFormatting options:\n[1] - User Data\n[2] - Tweet Data\n[3] - Hashtag Data\n"
echo -e "\nPost Formatting. Merge datasets:\n[1a] - Users\n[2a] - Tweets\n[3a] - Hashtags"
echo -ne "\nSelect a formatting option: "
# Number input option pattern
read OPTION

case $OPTION in

    1)
    # User data formatting jsonl conversion processing
    params='[.user.id_str, .id_str]'
    echo -e "\nFormatting user data jsonl conversion. Initial time: ${init}";
    formatFile "${source_files[file_1]}" "${params}" "${conversion_files[user_file_1]}";
    formatFile "${source_files[file_2]}" "${params}" "${conversion_files[user_file_2]}";
    ;;

    # Process to combine user datasets
    1a)
    removeDuplicates "${collect_files[user_files]}" "${combined_output[users_file]}"
    ;;

    2)
    # Tweet data formatting jsonl conversion processing
    echo -e "\nFormatting tweet data jsonl conversion. Initial time: ${init}";
    formatTweetFile "${source_files[file_1]}" "${process_files[tweet_file_1]}" "${conversion_files[tweet_file_1]}";
    formatTweetFile "${source_files[file_2]}" "${process_files[tweet_file_2]}" "${conversion_files[tweet_file_2]}";
    ;;

    # Process to combine tweet datasets
    2a)
    echo -e "\nCombining tweet datasets and deduplication."
    removeDuplicates "${collect_files[tweet_files]}" "${combined_output[tweets_file]}"
    ;;

    3)
    # Hashtag data formatting jsonl conversion processing
    params='{hashtag: .entities.hashtags[].text, tweet_id: .id_str} | [.hashtag, .tweet_id]'
    echo -e "\nFormatting hashtag data jsonl conversion. Initial time: ${init}";
    formatFile "${source_files[file_1]}" "${params}" "${conversion_files[hashtag_file_1]}";
    formatFile "${source_files[file_2]}" "${params}" "${conversion_files[hashtag_file_2]}";
    ;;

    # Process to combine hashtag datasets
    3a)
    removeDuplicates "${collect_files[hashtag_files]}" "${combined_output[hashtags_file]}"
    ;;

    *)
    echo -e "None selected\n"
    ;;

esac; \
timestampGen "${init}"