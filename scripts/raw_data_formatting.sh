#!/usr/bin/bash

declare -A source_files
declare -A process_files
declare -A output_files

# Data jsonl conversion to csv with formatting by defined attribute parameters.
# Replaces newline escape characters with tabs to prevent parsing errors
# and maintain csv delimitation and lining. eg tweet text overline spills over
# to newline, ends up becoming tweet id column when error causing newline escape is present.
# Latter removes duplicate data objects.
function formatTweetData() {
    cat $1 | jq -c '. | {tweet_id: .id_str, user_id: .user.id_str, date_created: .created_at, tweet_text: .full_text}' >> $2 \
    && sed 's/\\n/\\t/g' $2 | jq -cr ". | [.tweet_id, .user_id, .date_created, .tweet_text] | @csv" > $3 \
    && sort $3 -u | tr [:upper:] [:lower:] > $4 && echo "File created: $4";
}

# Data jsonl conversion to csv with formatting by defined attribute parameters.
# Latter removes duplicate data objects.
function formatHashtagData() {
    cat $1 | jq -cr '. | {hashtag: .entities.hashtags[].text, tweet_id: .id_str} | [.hashtag, .tweet_id] | @csv' >> $2 \
    && sort $2 -u > $3 && echo "File created: $3";
}

# Initial processing start time
init=$(date +'%s')

# Filepaths to datasets - in case of error due operating system, access bash scripts directly
# and change filepaths to "../data/" instead of "./data/"
source_files[set_1]="./data/raw/no_retweets*.jsonl"
source_files[process_type_1]="./data/raw/process/raw_no_retweets_${init}.jsonl"

process_files[type_1]="./data/raw/process/tweet_processing_${init}.csv"
process_files[type_2]="./data/raw/process/hashtag_processing_${init}.csv"

output_files[type_1]="./data/raw/output/tweet_data_${init}.csv"
output_files[type_2]="./data/raw/output/hashtag_data_${init}.csv"

echo -e "\nFormatting options:\n[1] - Format Tweet Data\n[2] - Format Hashtag Data"
echo -ne "\nSelect a formatting option: "
# Number input option pattern
read OPTION

case $OPTION in

    1)
    # Format Tweet Data
    echo -e "\nFormatting tweet data. Initial time: ${init}";
    formatTweetData "${source_files[set_1]}" "${source_files[process_type_1]}" "${process_files[type_1]}" "${output_files[type_1]}"
    ;;

    2)
    # Format Hashtag Data
    echo -e "\nFormatting hashtag data. Initial time: ${init}";
    formatHashtagData "${source_files[set_1]}" "${process_files[type_2]}" "${output_files[type_2]}"
    ;;

    *)
    echo -e "\nNone selected\n"
    ;;

esac;
duration=$[ $(date +'%s') - ${init} ] \
&& echo "Completion time: ${duration}"