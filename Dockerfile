# 第一阶段：编译
FROM golang:1.25-alpine AS builder

# 设置环境变量，使用国内镜像源（推荐七牛云或阿里）
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app
COPY . .

# 如果你的 go.mod 中有私有库，可能还需要设置 GONOPROXY
RUN go mod download

# 建议在构建时加上 CGO_ENABLED=0 以确保在 alpine 中完美运行
RUN CGO_ENABLED=0 go build -o master_bin ./master/master.go
RUN CGO_ENABLED=0 go build -o worker_bin ./worker/worker.go

# 第二阶段：运行
FROM alpine:latest
WORKDIR /root/
# RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
# RUN apk add --no-cache ca-certificates
COPY --from=builder /app/master_bin .
COPY --from=builder /app/worker_bin .

EXPOSE 8080 8081 8082 8083