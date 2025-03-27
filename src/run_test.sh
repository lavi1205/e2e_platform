#!/bin/bash

# Define paths
BASE_DIR="$(pwd)"
VENV_DIR="$BASE_DIR/venv_test"
SRC_DIR="$BASE_DIR"
TEST_DIR="$SRC_DIR/etl/test"
REQUIREMENTS="$SRC_DIR/requirements.txt"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    echo "Created virtual environment at $VENV_DIR"
fi 
source "$VENV_DIR/bin/activate"
if [ -f "$REQUIREMENTS" ]; then
    pip install -r "$REQUIREMENTS"
else
    echo "Error: requirements.txt not found at $REQUIREMENTS"
    exit 1
fi
echo "export py$PYTHONPATH"
# export PYTHONPATH="${PYTHONPATH}:$SRC_DIR"
export PYTHONPATH-"${PYTHONPATH}:$TEST_DIR"
echo "Display PYTHONPATH after setup success"
echo "$PYTHONPATH"
# export PYTHONPATH="${PYTHONPATH}:$SRC_DIR"
coverage run -m pytest -v -q etl/test/ && coverage report --show-missing
# coverage run -m pytest -v etl/test/ && coverage report --show-missing && coverage html && open htmlcov/index.html
deactivate
rm -rf "$VENV_DIR"
echo "Done"
