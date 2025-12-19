# --- STAGE 1: Build (Môi trường build) ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

# 1. Copy CHỈ CÁC file dependencies
# Điều này giúp tận dụng cache. Docker sẽ chỉ chạy lại bước 3
# nếu 2 file này thay đổi.
COPY go.mod .
COPY go.sum .

# 2. Tải dependencies dựa trên file go.sum
RUN go mod download

# 3. Copy toàn bộ mã nguồn còn lại
# (main.go, static/, v.v.)
COPY . .

# 4. Build Go binary
# Bây giờ, tất cả dependencies đã có sẵn
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /app/main .

# --- STAGE 2: Final (Môi trường chạy) ---
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/static ./static
COPY --from=builder /app/main .
EXPOSE 8080
CMD ["./main"]
