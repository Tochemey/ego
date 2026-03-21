# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy the entire ego repository so the replace directive (../../) resolves.
COPY . .

WORKDIR /src/example/cluster

RUN go mod download
RUN go mod tidy && go mod vendor
RUN CGO_ENABLED=0 GOOS=linux go build -o /app .

# Stage 2: Runtime
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app /app

EXPOSE 8080 9000 9001 9002

ENTRYPOINT ["/app"]
