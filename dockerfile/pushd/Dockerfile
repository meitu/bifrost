FROM golang:1.12

COPY . $GOPATH/src/github.com/meitu/bifrost

WORKDIR  $GOPATH/src/github.com/meitu/bifrost/push/bin/pushd

RUN GO111MODULE=on go build .

EXPOSE 5054 22345

WORKDIR  $GOPATH/src/github.com/meitu/bifrost/push

CMD bin/pushd/pushd-c conf/pushd.toml
