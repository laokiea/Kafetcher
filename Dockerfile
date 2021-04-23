FROM uhub.service.ucloud.cn/bluecity/golang:1.15.0-alpine as builder

WORKDIR /app

COPY ./ ./

RUN set -x; \
    mkdir /gopath \
    && unset GOPATH \
    && go env -w GOPATH=/gopath \
    && go env -w GO111MODULE=on \
    && go env -w GOPROXY=https://goproxy.cn,direct \
    && GOOS=linux GOARCH=amd64 go build -o ./kafetcher main.go

FROM uhub.service.ucloud.cn/bluecity/alpine:3.12

WORKDIR /app

COPY --from=builder /app/kafetcher ./
COPY --from=builder /app/config.json ./config.json

VOLUME kafetcher

CMD ["./kafetcher"]