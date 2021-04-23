FROM uhub.service.ucloud.cn/bluecity/golang:1.15.0-alpine as builder

WORKDIR /app

COPY ./ ./

RUN GOOS=linux GOARCH=amd64 go build -o ./kafetcher main.go

FROM uhub.service.ucloud.cn/bluecity/alpine:3.12

WORKDIR /app

COPY --from=builder /app/kafetcher ./

EXPOSE 8765

VOLUME kafetcher

CMD ["./kafetcher"]