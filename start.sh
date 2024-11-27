#!/bin/bash

# Variables for naming
IMAGE_NAME="rust_adapter_image"
CONTAINER_NAME="rust_adapter_container"
PORT=50051

# Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Check if the container is already running and stop it if necessary
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Stopping existing container..."
    docker stop $CONTAINER_NAME
fi

# Remove the existing container if it exists
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Removing existing container..."
    docker rm $CONTAINER_NAME
fi

# Run the Docker container
echo "Running Docker container..."
docker run -d -p $PORT:$PORT --name $CONTAINER_NAME $IMAGE_NAME

echo "Container $CONTAINER_NAME is running and listening on port $PORT."
