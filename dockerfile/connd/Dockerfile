FROM 172.16.200.19:5000/alpine/golang:latest

COPY commons $GOPATH/src/github.com/meitu/bifrost/commons
COPY grpc $GOPATH/src/github.com/meitu/bifrost/grpc
COPY version $GOPATH/src/github.com/meitu/bifrost/version
COPY vendor $GOPATH/src/github.com/meitu/bifrost/vendor
COPY connd $GOPATH/src/github.com/meitu/bifrost/connd

COPY connd/conf/connd.toml /bifrost/conf/connd.toml
COPY connd/data /bifrost/data

RUN go install github.com/meitu/bifrost/connd

EXPOSE 1884 50051 12345
WORKDIR /bifrost

CMD $GOPATH/bin/connd -c /bifrost/conf/connd.toml
