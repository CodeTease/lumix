# --- STAGE 1: Build ---
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /app/main .

# --- STAGE 2: Final ---
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/static ./static
COPY --from=builder /app/main .
EXPOSE 8080
CMD ["./main"]
