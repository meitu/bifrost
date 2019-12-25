#!/bin/bash
sha1=$(git log --pretty=format:'%h' -n 1 2>/dev/null)
branch=`git branch --contains | grep "* " | sed "s/\* //g"`

version="$branch"
if [ "$branch" == "master" ]; then
    tag=`git describe HEAD --tags`
    version=$tag
fi

if [ "$1" == "images" ]; then
    docker build ./ -t connd:$version -f dockerfile/connd/Dockerfile
    docker tag connd:$version bifrost/connd:intergration-test

    docker build ./ -t pushd:$version -f dockerfile/pushd/Dockerfile
    docker tag pushd:$version pushd:intergration-test
    exit
fi

name=bifrost-$version-$sha1

./build.sh

mkdir -p $name/bin/ $name/conf $name/data $name/logs
cp push/bin/pushd/pushd conn/bin/connd/connd $name/bin
cp push/conf/pushd.toml $name/conf/
cp conn/conf/connd.toml $name/conf/
#cp data/* $name/data

tar -zcf "$name".tar.gz $name
echo "$name".tar.gz "created"
if [ "$1" == "sfx" ];then
    cp sfx.installer $name/.sfx.installer
    misc/selfextractor $name
    echo "$name".bin "created"
fi

if [ "$1" == "rpm" ]; then
    mkdir -p ~/rpmbuild/SOURCES
    cp "$name".tar.gz ~/rpmbuild/SOURCES
    rpmbuild -bb bifrost.spec  -D "bifrost $name"
fi

