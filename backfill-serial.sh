#!/bin/zsh

setopt errexit
setopt nounset
setopt pipefail

start_date="1992-01-01"
end_date="1998-08-02"

# Convert dates to seconds since epoch
start_seconds=$(date -j -f "%Y-%m-%d" "$start_date" "+%s")
end_seconds=$(date -j -f "%Y-%m-%d" "$end_date" "+%s")
current_seconds=$start_seconds

while (( current_seconds <= end_seconds )); do
    current_date=$(date -r $current_seconds "+%Y-%m-%d")
    echo "Processing date: $current_date"
    python compute-summary.py $current_date
    
    # Check if the Python script executed successfully
    if [[ $? -ne 0 ]]; then
        echo "Error processing $current_date. Exiting."
        exit 1
    fi
    
    # Increment by one day (86400 seconds)
    (( current_seconds += 86400 ))
done

echo "All dates processed successfully."
