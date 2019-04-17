FROM golang:1.12-alpine3.9
RUN apk add --no-cache tini mariadb-client git
ENV GO111MODULE=on
ADD . /go/src/github.com/fasttrack-solutions/go-mysql-elasticsearch
RUN apk add --no-cache mariadb-client
RUN cd /go/src/github.com/fasttrack-solutions/go-mysql-elasticsearch/ && \
    go build -o bin/go-mysql-elasticsearch ./cmd/go-mysql-elasticsearch && \
    cp -f ./bin/go-mysql-elasticsearch /go/bin/go-mysql-elasticsearch
ENTRYPOINT ["/sbin/tini","--","go-mysql-elasticsearch"]
