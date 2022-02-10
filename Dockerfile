# Start from the latest golang base image
FROM golang:1.17-alpine as builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the command inside the container.
RUN CGO_ENABLED=0 GOOS=linux go build -v -o nolan


#Second stage build
FROM alpine:3.15

WORKDIR /root/

# Copy the binary to the production image from the builder stage.
COPY --from=builder /app .

# This container exposes port 80 to the outside world
EXPOSE 6969

ENTRYPOINT ["./nolan"]