#! usr/bin/bash

declare -A source_files
declare -A process_files

# Pre-selected Twitter API search terms
Health_terms=('insomnia' 'headache' 'stress')
Emotive_terms=('cry' 'anxiety' 'sad')

# Designated data extraction filepath
source_files[file_1]="./data/raw/tweets_1.jsonl"
source_files[file_2]="./data/raw/tweets_2.jsonl"

# Designated filepath retweet removal preprocessing filepath
process_files[file_1]="./data/raw/no_retweets_1.jsonl"
process_files[file_2]="./data/raw/no_retweets_2.jsonl"

function deselectRetweets() {
    # Function to deselect tweets where retweeted status is true with jq
    jq -c ' . | select(.retweeted_status == null)' $1 > $2
}

# Connect to historic Twitter API with twarc and filtering with jq
for i in "${!source_files[@]}"
do
    # Initiate processes by distinct key filepath (for: Health terms search parameter)
    if [[ "$i" == "file_1" ]]
    then
        echo "Searching Twitter API. Building file: ${source_files["$i"]}"
        # Data extraction from historic Twitter API search
        twarc search "'${Health_terms[0]} OR ${Health_terms[1]} OR ${Health_terms[2]}'" --lang en > ${source_files["$i"]}
        # Qualify filepath for raw data extraction exists
        if [[ -e ${source_files["$i"]} ]]
        echo "File: ${source_files["$i"]} created => Now processing.. please wait"
        then
            # Call to function
            deselectRetweets "${source_files[$i]}" "${process_files[$i]}"
            echo "File: ${process_files["$i"]} created"
        else
            echo "File: ${process_files["$i"]} has not been created."
        fi
    # Initiate processes by distinct key filepath (for: Emotive terms search parameter)
    elif [[ "$i" == "file_2" ]]
    then
        echo "Searching Twitter API. Building file: ${source_files["$i"]}"
        twarc search "'${Emotive_terms[0]} OR ${Emotive_terms[1]} OR ${Emotive_terms[2]}'" --lang en > ${source_files["$i"]}
        if [[ -e ${source_files["$i"]} ]]
        echo "File: ${source_files["$i"]} created => Now processing.. please wait"
        then
            deselectRetweets "${source_files[$i]}" "${process_files[$i]}"
            echo "File: ${process_files["$i"]} created"
        else
            echo "File: ${process_files["$i"]} has not been created."
        fi
    else
        echo "No file designated for data redirection."
    fi
done
