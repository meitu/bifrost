package util

/*
import (
	"fmt"
	"testing"

)

func TestEnqueue(t *testing.T) {
	rds := &conf.Redis{}
	rds.Masters = []string{"redis://127.0.0.1:6379"}
	rds.Slaves = [][]string{[]string{"redis://127.0.0.1:6379"}}
	rds.IdleTimeout = 240000
	rds.Timeout = 100

	c := conf.Queue{Redis: rds, Size: 1000}
	q, err := NewQueue(&c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = q.Enqueue("hello", &pbMessage.Message{Qos: 1, Payload: []byte("test")})
	if err != nil {
		t.Fatalf("Enqueue failed, %s", err)
	}
}

func TestGet(t *testing.T) {

	rds := &conf.Redis{}
	rds.Masters = []string{"redis://127.0.0.1:6379"}
	rds.Slaves = [][]string{[]string{"redis://127.0.0.1:6379"}}
	rds.IdleTimeout = 240000
	rds.Timeout = 100

	c := conf.Queue{Redis: rds, Size: 1000}
	q, err := NewQueue(&c)
	if err != nil {
		t.Fatalf("Enqueue failed, %s", err)
	}

	_, m, err := q.GetN("hello", EncodeInt64(1), 1000)
	if err != nil {
		t.Fatalf("Get failed, %s", err)
	}

	if m == nil {
		fmt.Printf("message is not exist\n")
	}
}
*/
