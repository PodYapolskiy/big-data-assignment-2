#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# using uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# Creating and acitvating virtual environment
if [ ! -d .venv ]; then
    uv venv --python 3.12  # python3 -m venv .venv
fi
source .venv/bin/activate

# Install any packages
uv pip install -r requirements.txt

# Package the virtual env if doesn't exist
if [ ! -f .venv.tar.gz ]; then
    uvx venv-pack -o .venv.tar.gz --compress-level 1
fi

# Collect data
# bash prepare_data.sh

# Run the indexer
# bash index.sh data/sample.txt

# Run the ranker
# bash search.sh "this is a query!"