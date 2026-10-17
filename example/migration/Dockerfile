# syntax=docker/dockerfile:1
# Builds the migration example's tool from the repository root, so the local
# eGo module and migration command are compiled in.
FROM golang:1.27.1-alpine AS build

WORKDIR /src
COPY . .

WORKDIR /src/example/migration
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -mod=mod -o /out/ego-migration .

FROM alpine:3.22
COPY --from=build /out/ego-migration /usr/local/bin/ego-migration
ENTRYPOINT ["ego-migration"]
