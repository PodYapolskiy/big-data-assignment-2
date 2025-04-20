#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit  \
    --archives /app/.venv.tar.gz#.venv \
    --jars /app/spark-cassandra-connector.jar \
    --packages com.github.jnr:jnr-posix:3.1.15 \
    query.py $1
    # --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.5.0 \
    # --master yarn \
