package store

import (
	"errors"

	"github.com/meitu/bifrost/push/conf"
)

var (
	ErrNoSubscriber = errors.New("no subscriber")
	ErrDeleteRetain = errors.New("delete retain")
)

type kvStore struct {
	cli Client
}

func NewTiStore(tconf *conf.Tikv) (Storage, error) {
	cli, err := newTiClient(tconf)
	if err != nil {
		return nil, err
	}
	s := &kvStore{
		cli: cli,
	}
	return s, nil

}

func NewRdsStore(rconf *conf.RedisStore) (Storage, error) {
	cli, err := newRdsClient(rconf)
	if err != nil {
		return nil, err
	}
	s := &kvStore{cli: cli}
	return s, nil
}

func (s *kvStore) Close() error {
	return s.cli.Close()
}
