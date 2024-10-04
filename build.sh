#!/bin/bash

# Variables
IMAGE_NAME="view-log-reporting"

# Build the application
echo "Building the Spring Boot application..."
mvn clean package -DskipTests

# Build Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Confirm that the image has been built
if [ $? -eq 0 ]; then
  echo "Docker image '$IMAGE_NAME' built successfully."
else
  echo "Failed to build the Docker image."
  exit 1
fi
