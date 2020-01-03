FROM golang:1.12

COPY . $GOPATH/src/github.com/meitu/bifrost

WORKDIR  $GOPATH/src/github.com/meitu/bifrost/conn/bin/connd

RUN GO111MODULE=on go build .

EXPOSE 1884 5051 12345

WORKDIR  $GOPATH/src/github.com/meitu/bifrost/conn

CMD bin/connd/connd -c conf/connd.toml
