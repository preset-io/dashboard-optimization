#!/bin/bash

PARALLEL_JOBS=4
MAX_RETRIES=3
COMMAND="python compute-summary.py"

# Start and end dates
START_DATE="1992-01-01"
END_DATE="1998-08-02"

# Create a function that retries the command if it fails
run_command() {
    local date=$1
    local retries=0

    while [ $retries -lt $MAX_RETRIES ]; do
        echo "Running command: $COMMAND $date (Attempt: $((retries + 1)))"
        $COMMAND $date

        if [ $? -eq 0 ]; then
            echo "Command succeeded: $COMMAND $date"
            return 0
        else
            echo "Command failed: $COMMAND $date. Retrying..."
            retries=$((retries + 1))
        fi
    done

    echo "Command failed after $MAX_RETRIES retries: $COMMAND $date"
    return 1
}

export -f run_command
export COMMAND MAX_RETRIES

# Generate all dates between START_DATE and END_DATE
seq_dates() {
    local current_date=$START_DATE
    local end_date=$END_DATE

    while [ "$current_date" != "$end_date" ]; do
        echo $current_date
        current_date=$(date -j -f "%Y-%m-%d" -v+1d "$current_date" "+%Y-%m-%d")
    done
    echo $end_date
}

# Use GNU parallel to run the commands in parallel
seq_dates | parallel -j $PARALLEL_JOBS run_command {}
