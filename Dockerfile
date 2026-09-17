FROM golang:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libzmq3-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /out/marketd ./cmd/marketd

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libzmq5 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/marketd /usr/local/bin/marketd
WORKDIR /app
COPY configs/docker.json /app/configs/local.json
USER 65532:65532
EXPOSE 5556 8080
ENTRYPOINT ["/usr/local/bin/marketd"]
CMD ["-config", "/app/configs/local.json"]
