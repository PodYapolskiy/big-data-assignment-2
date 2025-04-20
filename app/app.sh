#!/bin/bash
service ssh restart 
bash start-services.sh

# build cassandra-spark connector
bash build-connector.sh

# ensure valid python venv
if [ -d .venv ]; then
    rm -rf .venv
fi
python3 -m venv .venv
source .venv/bin/activate

# dependency installation and packaging forcely
pip install -r requirements.txt
venv-pack -f -o .venv.tar.gz

# data preparation
bash prepare_data.sh

# Run the indexer
# bash index.sh data/sample.txt

# Run the ranker
# bash search.sh "this is a query!"