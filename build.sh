#!/bin/bash

#generate the version info from git
scripts/version.sh

#generate the grpc protobuffer file
#(cd grpc && make clean && make)
#(cd pushd && make clean && make)

#run go test first
#go test ./...

#build the pushd & connd
if [ "$1" = "pushd" ] || [ $# = 0 ]; then
    echo building pushd/pushd
    (cd push/bin/pushd && go build .)
fi

if [ "$1" = "connd" ] || [ $# = 0 ]; then
    echo building connd/connd
    (cd conn/bin/connd && go build .)
fi
