FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /brokerd ./cmd/brokerd

# Alpine rather than distroless: compose runs a short setup script in it.
FROM alpine:3.22
RUN adduser -D -H brokerd
COPY --from=build /brokerd /usr/local/bin/brokerd
USER brokerd
EXPOSE 8080
ENTRYPOINT ["brokerd"]
CMD ["serve"]
