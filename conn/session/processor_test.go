package session

import (
	"testing"

	"github.com/arthurkiller/mqtt-go/packets"
	"github.com/stretchr/testify/assert"
)

func TestSessionHandleConnect(t *testing.T) {
	var (
		cpc        *packets.ConnectPacket
		cpcHandler func(t *testing.T, cp *packets.ConnectPacket, s *processor)

		cpw        *packets.ConnectPacket
		cpwHandler func(t *testing.T, cp *packets.ConnectPacket, s *processor)

		cpr        *packets.ConnectPacket
		cprHandler func(t *testing.T, cp *packets.ConnectPacket, s *processor)

		cpu        *packets.ConnectPacket
		cpuHandler func(t *testing.T, cp *packets.ConnectPacket, s *processor)
	)

	cpc = packets.NewConnectPacket()
	cpc.ProtocolName = "MQTT"
	cpc.ProtocolVersion = 4
	cpc.CleanSession = true
	cpc.Username = "bifrost-appkey=service-1544516816-1-bb7c77b22be1ff945d3272"
	cpcHandler = func(t *testing.T, cp *packets.ConnectPacket, s *processor) {
		assert.Equal(t, s.ctx.Service, "service")
		assert.NotEqual(t, s.ctx.ClientID, "")
		assert.Equal(t, s.ctx.CleanSession, true)
		assert.Equal(t, s.willFlag, false)
	}

	cpw = packets.NewConnectPacket()
	cpw = packets.NewConnectPacket()
	cpw.ProtocolName = "MQTT"
	cpw.ProtocolVersion = 4
	cpw.ClientIdentifier = "c2"
	cpw.WillFlag = true
	cpw.WillMessage = []byte("hehe")
	cpw.WillQoS = 1
	cpw.WillTopic = "topic"
	cpw.CleanSession = true
	cpw.Username = "bifrost-appkey=service-1544516816-1-bb7c77b22be1ff945d3272"
	cpwHandler = func(t *testing.T, cp *packets.ConnectPacket, s *processor) {
		assert.Equal(t, s.ctx.Service, "service")
		assert.Equal(t, s.ctx.ClientID, "c2")
		assert.Equal(t, s.ctx.CleanSession, true)
		assert.Equal(t, s.willFlag, true)
		assert.Equal(t, s.willMessage.Qos, int32(cpw.WillQoS))
		assert.Equal(t, s.willMessage.Topic, cpw.WillTopic)
		assert.Equal(t, s.willMessage.Retain, cpw.WillRetain)
		assert.Equal(t, s.willMessage.Payload, cpw.WillMessage)
	}

	cpu = packets.NewConnectPacket()
	cpu.ProtocolName = "MQTT"
	cpu.ProtocolVersion = 4
	cpu.CleanSession = true
	cpu.ClientIdentifier = "c1"
	cpuHandler = func(t *testing.T, cp *packets.ConnectPacket, s *processor) {
		assert.Equal(t, s.ctx.ClientID, "c1")
		assert.Equal(t, s.ctx.CleanSession, true)
		assert.Equal(t, s.willFlag, false)
	}

	cpr = packets.NewConnectPacket()
	cpr.ProtocolName = "MQTT"
	cpr.ProtocolVersion = 4
	cpr.CleanSession = false
	cpr.Username = "bifrost-appkey=service-1544516816-1-bb7c77b22be1ff945d3272"
	cpr.ClientIdentifier = "c1"
	cprHandler = func(t *testing.T, cp *packets.ConnectPacket, s *processor) {
		assert.Equal(t, s.ctx.Service, "service")
		assert.Equal(t, s.ctx.ClientID, "c1")
		assert.Equal(t, s.ctx.CleanSession, false)
		assert.Equal(t, s.willFlag, false)
	}

	tests := []struct {
		name string
		args *packets.ConnectPacket
		want func(t *testing.T, cp *packets.ConnectPacket, s *processor)
	}{
		{
			name: "cleansession",
			args: cpr,
			want: cprHandler,
		},
		{
			name: "clientid-null",
			args: cpc,
			want: cpcHandler,
		},
		{
			name: "usrname-null",
			args: cpu,
			want: cpuHandler,
		},
		{
			name: "will",
			args: cpw,
			want: cpwHandler,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewProcessor(sctx, ss.Notify)
			_, _, err := s.handleConnect(tt.args)
			assert.NoError(t, err)
			tt.want(t, cpc, s)
		})
	}

}

func TestHandlePacket(t *testing.T) {

	subperr := packets.NewSubscribePacket()
	subp := packets.NewSubscribePacket()
	subp.MessageID = 10
	subp.Topics = []string{"t1", "t2"}
	subp.QoSs = []byte{byte(0), byte(1)}
	subpa := packets.NewSubackPacket()
	subpa.MessageID = 10

	unsubperr := packets.NewUnsubscribePacket()
	unsubp := packets.NewUnsubscribePacket()
	unsubp.MessageID = 10
	unsubp.Topics = []string{"t1", "t2"}
	unsubpa := packets.NewUnsubackPacket()
	unsubpa.MessageID = 10

	pubp := packets.NewPublishPacket()
	pubp.TopicName = "t"
	pubp.Payload = []byte("hehe")
	pubp.QoS = byte(1)
	pubp.MessageID = 1234
	ackp := packets.NewPubackPacket()
	ackp.MessageID = 1234
	// pp := packets.NewPingrespPacket()

	tests := []struct {
		name    string
		args    packets.ControlPacket
		want    packets.ControlPacket
		wanterr error
	}{
		{
			name:    "suberr",
			args:    subperr,
			wanterr: ErrTopicsCount,
		},
		{
			name: "sub",
			args: subp,
			want: subpa,
		},
		{
			name:    "unsuberr",
			args:    unsubperr,
			wanterr: ErrTopicsCount,
		},
		{
			name: "unsub",
			args: unsubp,
			want: unsubpa,
		},
		{
			name: "pub",
			args: pubp,
			want: ackp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewProcessor(sctx, ss.Notify)
			err := s.handlePacket(tt.args)
			if tt.wanterr != nil {
				assert.Equal(t, tt.wanterr, err)
				return
			}
			assert.NoError(t, err)
			assert.Contains(t, s.ctx.GetAndClearWriteBuffer()[0].String(), tt.want.String())
		})
	}
}

func TestHandleDisconnect(t *testing.T) {
	p := packets.NewDisconnectPacket()
	s := NewProcessor(sctx, ss.Notify)
	err := s.handleDisconnect(p)
	assert.NoError(t, err)
}
