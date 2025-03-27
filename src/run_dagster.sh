#!/bin/bash

# Script to build and run Dagster Docker container

# Variables
IMAGE_NAME="e2e-dagster"
CONTAINER_NAME="dagster-container"
PORT=3000

echo "Building Docker image: $IMAGE_NAME..."
docker build -t "$IMAGE_NAME" .

echo "Checking for existing container: $CONTAINER_NAME..."
if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping and removing existing container..."
    docker stop "$CONTAINER_NAME" && docker rm "$CONTAINER_NAME"
fi

echo "Running Docker container: $CONTAINER_NAME..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:$PORT" --restart always \
    "$IMAGE_NAME"
