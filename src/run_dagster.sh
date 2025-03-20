#!/bin/bash

# Script to build and run Dagster Docker container

# Variables
IMAGE_NAME="e2e-dagster"
CONTAINER_NAME="dagster-container"
PORT=3000

# Build the Docker image
echo "Building Docker image: $IMAGE_NAME..."
docker build -t "$IMAGE_NAME" .

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "Docker image built successfully."
else
    echo "Error: Docker image build failed."
    exit 1
fi

# Stop and remove any existing container with the same name
echo "Checking for existing container: $CONTAINER_NAME..."
if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping and removing existing container..."
    docker stop "$CONTAINER_NAME" && docker rm "$CONTAINER_NAME"
fi

# Run the Docker container
echo "Running Docker container: $CONTAINER_NAME..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:$PORT" \
    "$IMAGE_NAME"

# Check if container is running
if [ $? -eq 0 ]; then
    echo "Dagster is running at http://localhost:$PORT"
    echo "To view logs, run: docker logs $CONTAINER_NAME"
    echo "To stop the container, run: docker stop $CONTAINER_NAME"
else
    echo "Error: Failed to start the container."
    exit 1
fi