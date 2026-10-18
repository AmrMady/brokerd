FROM golang:1.26-alpine AS build
# The outbox's Kafka client is librdkafka, through cgo.
RUN apk add --no-cache build-base
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /brokerd ./cmd/brokerd
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -tags musl -o /outbox ./cmd/outbox

# Alpine rather than distroless: compose runs a short setup script in it,
# and the outbox binary links against musl.
FROM alpine:3.22
RUN adduser -D -H brokerd
COPY --from=build /brokerd /outbox /usr/local/bin/
USER brokerd
EXPOSE 8080
ENTRYPOINT ["brokerd"]
CMD ["serve"]
