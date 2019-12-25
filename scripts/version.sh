#!/bin/bash

#Generate git info

trap exit ERR SIGINT

sha1=`git log --abbrev-commit --oneline -n 1 | awk '{print $1}'`
log=`git log --abbrev-commit --oneline -n 1 | sed "s/$sha1//g"`
branch=`git branch --contains | grep "* " | sed "s/\* //g"`
tag=`git tag  --contains`

cat > version/init.go << CODE
package version

func init() {
    version.Sha1 = "$sha1"
    version.LastCommit = "$log"
    version.Branch = "$branch"
    version.Tag = "$tag"
}
CODE
echo version/init.go created
