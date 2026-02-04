# Stage 1: Build 
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /app/lith-ranker .

# Stage 2: Run 
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/lith-ranker .

CMD ["./lith-ranker"]