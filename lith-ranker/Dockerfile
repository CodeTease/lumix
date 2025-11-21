# Stage 1: Build (Tạo binary nhỏ gọn)
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

# Build binary tên là "lith-ranker"
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /app/lith-ranker .

# Stage 2: Run (Môi trường chạy siêu nhẹ)
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/lith-ranker .

# Chạy Lith Ranker
CMD ["./lith-ranker"]