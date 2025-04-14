#!/bin/bash
# index.sh: Build the index by running the MapReduce pipeline and store data in Cassandra
# Usage: ./index.sh [path_to_input]
#   If no argument is provided, default input is /index/data in HDFS.

# Set input path: either passed as argument or default to /index/data
INPUT_PATH="$1"
if [ -z "$INPUT_PATH" ]; then
    INPUT_PATH="/index/data"
    echo "No input path provided. Using default HDFS folder: $INPUT_PATH"
else
    # If the provided file exists locally, put it in HDFS under /index/data
    if [ -f "$INPUT_PATH" ]; then
        echo "Local file provided. Copying $INPUT_PATH to HDFS /index/data/"
        hdfs dfs -mkdir -p /index/data
        hdfs dfs -put -f "$INPUT_PATH" /index/data/
        INPUT_PATH="/index/data/$(basename "$INPUT_PATH")"
    else
        echo "Using provided HDFS path: $INPUT_PATH"
    fi
fi

OUTPUT_DIR="/tmp/index"  # Set output directory for intermediate MapReduce results
hdfs dfs -rm -r -f $OUTPUT_DIR  # Remove any previous output directory

echo "Starting Hadoop MapReduce job with:"
mapred streaming \
    -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py" \
    -mapper "/app/.venv/bin/python3 mapreduce/mapper1.py" \
    -reducer "/app/.venv/bin/python3 mapreduce/reducer1.py" \
    -input "$INPUT_PATH/*" \
    -output "$OUTPUT_DIR" \
    # -D mapred.map.tasks=1 \
    # -D mapred.reduce.tasks=0 \

# Check the job status and print a message
if [ $? -eq 0 ]; then
    echo "Indexing completed successfully. Check Cassandra for updated index tables."
else
    echo "Indexing job failed. Please review the error messages above."
fi
