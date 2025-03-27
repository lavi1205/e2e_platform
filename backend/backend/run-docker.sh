#!/bin/bash

# Script to build and run the FastAPI Docker container

# Variables
IMAGE_NAME="fastapi-app"
CONTAINER_NAME="fastapi-container"
PORT=80

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

check_status() {
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: $1 failed${NC}"
        exit 1
    fi
}

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

echo "Building Docker image: ${IMAGE_NAME}..."
docker build -t ${IMAGE_NAME} .
check_status "Docker build"

if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo "Stopping existing container..."
    docker stop ${CONTAINER_NAME}
    check_status "Stopping existing container"
    docker rm ${CONTAINER_NAME}
    check_status "Removing existing container"
fi
echo "Running Docker container: ${CONTAINER_NAME}..."
docker run -d \
    --name ${CONTAINER_NAME} \
    -p ${PORT}:80 \
    --restart always \
    ${IMAGE_NAME}

check_status "Docker run"

sleep 2

if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo -e "${GREEN}Container is running successfully!${NC}"
fi

exit 0