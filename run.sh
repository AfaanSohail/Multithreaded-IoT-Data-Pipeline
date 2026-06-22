#!/bin/bash

# Create variables with values 
inputDir="data"
outputDir="output"
numThreads=4
queueSize=100
fifoPath="/tmp/my_fifo"
shmName="/my_shm"

# Global variable to track our C++ process
dispatcherPid=""

showUsage() {
    echo "How to use this script: $0 [-i inputDir] [-o outputDir] [-n numThreads] [-q queueSize] [-f fifoPath] [-s shmName] [-c] [-h]"
    echo "  -i : Folder where your CSVs live (default: data/)"
    echo "  -o : Folder where reports will be saved (default: output/)"
    echo "  -n : How many worker threads to use (default: 4)"
    echo "  -q : Queue size for the bounded buffer (default: 100)"
    echo "  -f : Named pipe path (default: /tmp/my_fifo)"
    echo "  -s : Shared memory name (default: /my_shm)"
    echo "  -c : Clean up old compiled files and logs"
    echo "  -h : Show this helpful message"
    exit 10 
}

cleanProject() {
    echo -e "\n[Orchestrator] Stopping the project and Cleaning up."
    
    # Checking if the variable is not empty, then kill it cleanly
    if [ -n "$dispatcherPid" ]; then
        kill "$dispatcherPid"
    fi
    
    if [ -f .pid ]; then
        rm -f .pid
    fi
    
    echo "[Orchestrator] Clean done complete."
}

printSummary() {
    local startTime=$1
    local endTime=$(date +%s)
    local runTime=$((endTime - startTime))
    
    echo "PIPELINE SUMMARY:"
    echo "->Total Runtime: $runTime seconds"
    echo "->Final Exit Status: $?"
}

trap cleanProject EXIT INT TERM

# Saving the arguments coming from terminal command
while getopts "i:o:n:q:f:s:ch" opt; do
    case ${opt} in
        i ) inputDir=$OPTARG ;;
        o ) outputDir=$OPTARG ;;
        n ) numThreads=$OPTARG ;;
        q ) queueSize=$OPTARG ;;
        f ) fifoPath=$OPTARG ;;
        s ) shmName=$OPTARG ;;
        c ) make clean; exit 0 ;;
        h ) showUsage ;;
        * ) showUsage ;;
    esac
done

# Checking if g++ and make is there
if ! command -v gcc || ! command -v make; then
    echo "[Error] You need to install gcc and make to run this."
    exit 40 
fi

# Checking if there is a input folder
if [ ! -d "$inputDir" ]; then
    echo "[Error] We cannot find the input directory: '$inputDir'."
    exit 40
fi

# Creating output if it doesn't exist
if [ ! -d "$outputDir" ]; then
    mkdir -p "$outputDir"
fi
if [ ! -d "logs" ]; then
    mkdir -p logs
fi

echo "[Orchestrator] Compiling the C++ project now."
make

if [ $? -ne 0 ]; then
    echo "[Error] The code failed to compile. Please check your C++ files."
    exit 30 
fi

echo "[Orchestrator] Build successful! Waking up the dispatcher..."

# saving all csv files name in imput.txt
ls "$inputDir"/*.csv > input.txt

startTime=$(date +%s)

# starting the dispatcher with all the parameters
./src/dispatcher -i "$inputDir" -o "$outputDir" -n "$numThreads" -q "$queueSize" -f "$fifoPath" -s "$shmName" &

dispatcherPid=$!
echo $dispatcherPid > .pid

wait $dispatcherPid

printSummary $startTime
