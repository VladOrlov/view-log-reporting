#!/bin/bash

# Variables
IMAGE_NAME="view-log-reporting"
CONTAINER_NAME="view-log-reporting-container"
SPRING_PROFILE="${1:-default}"  # First argument is the spring profile, default is 'default'
PORT="${2:-8080}"  # Second argument is the port, default is 8080
TAIL_LOGS="${3:-false}"  # Third argument is whether to tail logs or not, default is false

# Build the application
echo "Building the Spring Boot application..."
mvn clean package -DskipTests

# Build Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Stop and remove any existing container with the same name
echo "Stopping and removing existing container (if any)..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

# Run Docker container with the specified Spring profile and port
echo "Running Docker container..."
docker run -d --name $CONTAINER_NAME \
  --network kafka-network \
  -e SPRING_PROFILES_ACTIVE=$SPRING_PROFILE \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -p $PORT:8080 \
  $IMAGE_NAME

# Check if the container started successfully
if [ $? -eq 0 ]; then
  echo "Container is running with profile '$SPRING_PROFILE' and port '$PORT'."

  # Optionally tail logs if the third argument is true
  if [ "$TAIL_LOGS" = "true" ]; then
    echo "Tailing container logs..."
    docker logs -f $CONTAINER_NAME
  else
    echo "To tail logs, re-run the script with the third argument as 'true'."
  fi
else
  echo "Failed to start the container."
  exit 1
fi
