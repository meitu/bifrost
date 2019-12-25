FROM 172.16.200.19:5000/alpine/golang:latest

COPY commons $GOPATH/src/github.com/meitu/bifrost/commons
COPY grpc $GOPATH/src/github.com/meitu/bifrost/grpc
COPY version $GOPATH/src/github.com/meitu/bifrost/version
COPY vendor $GOPATH/src/github.com/meitu/bifrost/vendor
COPY pushd $GOPATH/src/github.com/meitu/bifrost/pushd

RUN go install github.com/meitu/bifrost/pushd

COPY pushd/conf/pushd.toml /bifrost/conf/pushd.toml

WORKDIR /bifrost
EXPOSE 50054 22345

CMD $GOPATH/bin/pushd -c /bifrost/conf/pushd.toml
