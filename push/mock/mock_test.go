package mock

import (
	"testing"

	"github.com/meitu/bifrost/misc/autotest/pushdtest/testcase"
	"github.com/stretchr/testify/assert"
)

func TestMock(t *testing.T) {
	/*
		clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		etcdAddrs := clus.RandClient().Endpoints()
		mock := NewMockPushd(t, etcdAddrs)
		mock.Start(t)
		time.Sleep(time.Second)
		mock.Stop(t)
		clus.Terminate(t)
	*/
}

func TestCase(t *testing.T) {
	addrs := []string{"127.0.0.1:12342", "127.0.0.1:61421"}
	cli, err := testcase.NewClient(etcdAddrs, addrs, mock.Config.Server.Push.Service, mock.Config.Server.Push.Group)
	assert.NoError(t, err)
	err = cli.Thord()
	assert.NoError(t, err)
	err = cli.IM()
	assert.NoError(t, err)
	err = cli.CleanSession()
	assert.NoError(t, err)
	err = cli.Retain(mock.Config.Server.Publish.Service)
	assert.NoError(t, err)
	err = cli.Publish(mock.Config.Server.Publish.Service)
	assert.NoError(t, err)
	err = cli.Repeat()
	assert.NoError(t, err)
	err = cli.Kick()
	assert.NoError(t, err)
	err = cli.KickClean()
	assert.NoError(t, err)
	err = cli.CallbackFunc(mock.Config.Server.Callback.Service)
	assert.NoError(t, err)
}
