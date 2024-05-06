# Dockerfile

# Use a Golang base image
FROM golang:latest

# Set the current working directory inside the container
WORKDIR /app

# Copy the local code to the container
COPY . .

# Build the Go application
RUN go build -o producer .

# Command to run the executable
CMD ["./producer"]
