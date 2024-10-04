FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy the built JAR file to the container
COPY target/view-log-reporting-0.1.0.jar /app/view-log-reporting-0.1.0.jar

# Expose the default port Spring Boot runs on
EXPOSE 8080

# Set default environment variables
ENV SPRING_PROFILES_ACTIVE=default
ENV SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV OUTPUT_PATH=/app/output

# Create the output directory in the container (if it doesn't exist)
RUN mkdir -p /app/output

# Add an entrypoint to run the Spring Boot application
ENTRYPOINT ["java", "-jar", "/app/view-log-reporting-0.1.0.jar"]

# Allow passing additional arguments to the Java command
CMD ["--spring.profiles.active=${SPRING_PROFILES_ACTIVE}", \
     "--spring.kafka.bootstrap-servers=${SPRING_KAFKA_BOOTSTRAP_SERVERS}", \
     "--output.path=${OUTPUT_PATH}"]
