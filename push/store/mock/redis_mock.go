package mock

import (
	"log"

	"github.com/meitu/bifrost/push/conf"
	util "github.com/meitu/bifrost/push/store/redis_util"
)

func MockRedis() util.Redic {
	return util.New(
		&conf.Redis{
			Masters: []string{"redis://127.0.0.1:6379"},
			Slaves:  [][]string{[]string{"redis://127.0.0.1:6379"}},
		})
}

func MockQueue() util.Queue {
	qconf := conf.Queue{
		Redis: &conf.Redis{
			Masters: []string{"redis://127.0.0.1:6379"},
			Slaves:  [][]string{[]string{"redis://127.0.0.1:6379"}},
		},
		Size:            100,
		AbortOnOverflow: false,
		Expire:          20,
	}
	queue, err := util.NewQueue(&qconf)
	if err != nil {
		log.Fatal(err)
	}
	return queue
}
