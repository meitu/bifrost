package callback

import (
	"context"
	"fmt"
	"os"

	"github.com/distributedio/configo"
	"github.com/meitu/bifrost/push/conf"
)

var service string

type MockCallback struct {
	callback
}

func NewMockCallback(addrs []string) Callback {
	path := os.Getenv("GOPATH")
	path = path + "/src/github.com/meitu/bifrost/push/conf/pushd.toml"
	var config conf.Pushd
	if err := configo.Load(path, &config); err != nil {
		fmt.Printf("load config file failed , %s\n", err)
		os.Exit(1)
	}
	config.Server.Etcd.Cluster = addrs
	service = config.Server.Callback.Service
	config.ValidateConf()
	cb, _ := NewCallback(&config.Server.Callback, context.Background())
	return cb
}
