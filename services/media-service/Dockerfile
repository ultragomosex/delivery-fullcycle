FROM golang:alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/media-service main.go

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /app/bin/media-service /app/media-service

EXPOSE 8081
USER nonroot:nonroot

ENTRYPOINT ["/app/media-service"]
