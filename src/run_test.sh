#!/bin/bash

# Script to set up venv and run tests with pytest

# Variables
VENV_DIR=".venv_test"  # Separate venv for tests to avoid conflicts
TEST_DIR="etl/test"
SRC_DIR="$(pwd)"  # Set to current directory (src/), assuming script runs from there
echo $SRC_DIR
# Create and activate virtual environment
echo "Setting up virtual environment in $VENV_DIR..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

# Install requirements
echo "Installing requirements..."
pip install --upgrade pip
pip install -r requirements.txt

# Set PYTHONPATH to include the src directory
# export PYTHONPATH="$SRC_DIR/etl:$PYTHONPATH"
export PYTHONPATH="$SRC_DIR"
# export PYTHONPATH="$SRC_DIR/etl:$PYTHONPATH"
echo "$PYTHONPATH"
echo "PYTHONPATH set to: $PYTHONPATH"

# Run tests with pytest
echo "Running tests in $TEST_DIR..."
pytest "$TEST_DIR" -v
TEST_STATUS=$?

# Deactivate virtual environment
deactivate

# Check test results
if [ $TEST_STATUS -eq 0 ]; then
    echo "All tests passed successfully!"
else
    echo "Error: Some tests failed."
    exit 1
fi