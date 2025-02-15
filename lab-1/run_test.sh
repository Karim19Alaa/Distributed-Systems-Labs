#!/bin/bash

# Check if at least three arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 [cpu|io] [port_number] [number_of_threads]"
    exit 1
fi

# Assign arguments to variables
test_type=$1
port_number=$2
num_threads=$3

# Determine which JMeter script to run based on the first argument
case $test_type in
    cpu)
        script="tests/CPU_Bound.jmx"
        ;;
    io)
        script="tests/IO_Bound.jmx"
        ;;
    *)
        echo "Error: First argument must be 'cpu' or 'io'."
        exit 1
        ;;
esac

# Run the selected JMeter script with the provided port number and number of threads
jmeter -n -t "$script" -Jport="$port_number" -JnThreads="$num_threads"