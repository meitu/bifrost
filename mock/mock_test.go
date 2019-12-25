package mock

import (
	"testing"

	"github.com/meitu/bifrost/misc/autotest/conndtest/mqtttest"
)

func ATestMQTT(t *testing.T) {
	cli := mqtttest.NewClient(publishAddr, mqttAddr[0])
	if err := cli.CleanSession(); err != nil {
		t.Errorf("cleansession failed : %s \n", err)
	}
	if err := cli.Retain(); err != nil {
		t.Errorf("Retain failed : %s \n", err)
	}
	if err := cli.IMLiveCase(); err != nil {
		t.Errorf("IMLive failed : %s \n", err)
	}
	if err := cli.PushCase(); err != nil {
		t.Errorf("Push failed : %s \n", err)
	}
}

// TODO
func TestCallback(t *testing.T) {
	/*
		callcli := callbacktest.NewClient(publishAddr, mqttAddr[0], etcdAddrs)
		if err := callcli.Service(callbackService); err != nil {
			t.Errorf("Service failed , %s \n", err)
		}
			if err := callcli.CookieEuqal(callbackService); err != nil {
				t.Errorf("Cookie failed , %s \n", err)
			}
	*/
}
