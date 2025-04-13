#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# using uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# Creating a virtual environment
uv venv --python 3.12
# python3 -m venv .venv
source .venv/bin/activate

# Install any packages
uv pip install -r requirements.txt

# Package the virtual env.
uvx venv-pack -o .venv.tar.gz

# Collect data
# bash prepare_data.sh

# Run the indexer
# bash index.sh data/sample.txt

# Run the ranker
# bash search.sh "this is a query!"