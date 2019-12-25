package conf

import (
	"encoding/json"
	"errors"
	"time"
)

type Connd struct {
	PIDFileName string     `cfg:"pid-filename; connd.pid; ; the file name to record connd PID"`
	Logger      Logger     `cfg:"logger"`
	Etcd        Etcd       `cfg:"etcd"`
	Pubsub      Pubsub     `cfg:"pubsub"`
	Push        PushClient `cfg:"push-client"`

	Grpc       Grpc              `cfg:"grpc"`
	MqttServer MqttServer        `cfg:"mqtt-server"`
	Monitor    Monitor           `cfg:"monitor"`
	Radars     []*Radar          `cfg:"-"`
	Listen     []*ListenerConfig `cfg:"-"`

	Tcp ListenerConfig `cfg:"tcp"`
	Tls ListenerConfig `cfg:"tls"`
	Ws  ListenerConfig `cfg:"http"`
	Wss ListenerConfig `cfg:"https"`
}

type Logger struct {
	DebugLog     DebugLogger `cfg:"debug-logger"`
	AccessLog    DebugLogger `cfg:"access-logger"`
	StatisticLog DebugLogger `cfg:"statistic-logger"`
}

type DebugLogger struct {
	Name       string `cfg:"name; connd_debug; ; the default logger name"`
	Path       string `cfg:"path; logs/connd; ; the default log path"`
	Format     string `cfg:"format;2006-01-02 15:04:05.999999999; ; log time format"`
	Level      string `cfg:"level; info; ; log level(debug, info, warn, error, panic, fatal)"`
	TimeRotate string `cfg:"time-rotate; 0 0 0 * * *; ; log time rotate pattern(s m h D M W)"`
	Compress   bool   `cfg:"compress; false; boolean; true for enabling log compress"`
}

type MqttServer struct {
	Etcd    *Etcd
	Session Session `session`
}

type Session struct {
	DefaultKeepalive time.Duration `cfg:"default-keepalive; 5m0s; ; default keepalive time, defaule unit minute"`
	MaxKeepalive     time.Duration `cfg:"max-keepalive; 10m0s; ; max keepalive time"`
	WriteChanSize    int           `cfg:"write-chan-size; 64; ; write chan size"`
	PullLimit        int64         `cfg:"pull-limit; 100; ; pull request limitation"`
	RangeUnackLimit  int64         `cfg:"range-unack-limit; 50; ; range unack message limitation"`
	Auth             string        `cfg:"auth;;;client connetion auth"`
	PacketSizeLimit  int           `cfg:"packet-size-limit; 1048576; ; default is 1024*1024 Byte"`
}

type Pubsub struct {
	Topic Topic `cfg:"topic"`
}

// TODO
type Topic struct {
	CacheEnabled       bool          `cfg:"cache-enable; false; boolean; enable cache opentracing"`
	CacheBufferSize    int           `cfg:"cache-size; 1000;; enable cache if topic subs over than threshold"`
	DiscardThreshold   int           `cfg:"topicmap-threshold; 100; ; topic map downgrade threshold"`
	DowngradeLimit     time.Duration `cfg:"downgrade-limit; 500ms; ; cancel scan if downgrade exceeded"`
	DowngradeThreshold int           `cfg:"downgrade-threshold; 1000; ; cancel scan if downgrade exceeded"`
}

type PushClient struct {
	Service          string   `cfg:"service; pushd;nonempty; service name of pushd"`
	Group            string   `cfg:"group; group;nonempty; group name of pushd"`
	Region           string   `cfg:"region; region;nonempty; region name of pushd"`
	AppKey           string   `cfg:"appkey; appkey;; appkey of pushd"`
	MinProviderNodes uint32   `cfg:"min-privider-nodes; 3; numeric; the min number of pushd nodes"`
	Cluster          []string `cfg:"cluster; []; netaddr; addresses of pushd cluser, discoverd from etcd if this option is empty or omited"`

	PushdRetryTimes      int           `cfg:"pushd-retry-times; 3; >0;call the pushd grpc retry times"`
	DefaultTimeout       time.Duration `cfg:"default-timeout; 600ms; ; default time out"`
	BackoffTime          time.Duration `cfg:"backoff-time; 100ms; ; packoff time"`
	RequestSlowThreshold time.Duration `cfg:"request-slow-threshold; 100ms; ; slow log threshold for request to pushd"`
	Etcd                 *Etcd
}

type Radar struct {
	Etcd       *Etcd
	Path       string `cfg:"path; path;nonempty; path name of radar"`
	Addr       string `cfg:"addr; ;nonempty; address of radar"`
	Name       string `cfg:"name; name;nonempty; name of radar"`
	Region     string `cfg:"region; region;nonempty; region of radar"`
	MaxConnCnt int    `cfg:"max-conn; 100000;; max connection count"`
}

type ListenerConfig struct {
	Type         string `cfg:"type;;;connect type[tls tcp ws wss]"`
	Listen       string `cfg:"listen;;; listen address"`
	Region       string `cfg:"region;beijing;; default region"`
	RegisterAddr string `cfg:"register-addr;;; register address to etcd"`
	Pem          string `cfg:"pem;;; pem file name"`
	Key          string `cfg:"key;;; key file name"`
	TicketKey    string `cfg:"ticketkey;;; tls session ticket file name. ticket use: openssl rand 32"`
	ServiceName  string `cfg:"servname;service;nonempty; register to etcd"`
	MaxConnCnt   int    `cfg:"max-conn-cnt; 300000; ; max connect client num"`
}

type Grpc struct {
	Listen              string        `cfg:"listen; 127.0.0.1:5051; netaddr; gprc listen addr"`
	NotifySlowThreshold time.Duration `cfg:"notify-slow-threshold; 100ms; ; slow log threshold for notfiy"`
}

type Etcd struct {
	Cluster  []string `cfg:"cluster;['http://127.0.0.1:2379']; nonempty; etcd cluster endpoints"`
	Username string   `cfg:"username;;; username of etcd"`
	Password string   `cfg:"password;;; password of the user"`
}

//is a http server to export expvar, grpctrace and prometheus metrics
type Monitor struct {
	Listen string `cfg:"listen;0.0.0.0:12345;; listen address of the status server"`
	Enable bool   `cfg:"enable; false; boolean; enable opentracing"`
}

func (c *Connd) String() string {
	content, _ := json.Marshal(*c)
	return string(content)
}

// 处理etcd 和radar 赋值问题
func (c *Connd) initServer() {
	//TODO 按照一定规则进行配置管理
	c.MqttServer.Etcd = &c.Etcd
	c.Push.Etcd = &c.Etcd

	if c.Tcp.RegisterAddr == "" {
		c.Tcp.RegisterAddr = c.Tcp.Listen
	}
	if c.Tls.RegisterAddr == "" {
		c.Tls.RegisterAddr = c.Tls.Listen
	}
	if c.Ws.RegisterAddr == "" {
		c.Ws.RegisterAddr = c.Ws.Listen
	}
	if c.Wss.RegisterAddr == "" {
		c.Wss.RegisterAddr = c.Wss.Listen
	}

	// 更新radar
	if c.Tcp.Listen != "" {
		c.Radars = append(c.Radars, &Radar{
			Etcd:       &c.Etcd,
			Path:       c.Tcp.ServiceName,
			Addr:       c.Tcp.RegisterAddr,
			Name:       c.Tcp.Type,
			Region:     c.Tcp.Region,
			MaxConnCnt: c.Tcp.MaxConnCnt,
		})
		c.Listen = append(c.Listen, &c.Tcp)
	}
	if c.Tls.Listen != "" {
		c.Radars = append(c.Radars, &Radar{
			Etcd:       &c.Etcd,
			Path:       c.Tls.ServiceName,
			Addr:       c.Tls.RegisterAddr,
			Name:       c.Tls.Type,
			Region:     c.Tls.Region,
			MaxConnCnt: c.Tls.MaxConnCnt,
		})
		c.Listen = append(c.Listen, &c.Tls)
	}
	if c.Ws.Listen != "" {
		c.Radars = append(c.Radars, &Radar{
			Etcd:       &c.Etcd,
			Path:       c.Ws.ServiceName,
			Addr:       c.Ws.RegisterAddr,
			Name:       c.Ws.Type,
			Region:     c.Ws.Region,
			MaxConnCnt: c.Ws.MaxConnCnt,
		})
		c.Listen = append(c.Listen, &c.Ws)
	}

	if c.Wss.Listen != "" {
		c.Radars = append(c.Radars, &Radar{
			Etcd:       &c.Etcd,
			Path:       c.Wss.ServiceName,
			Addr:       c.Wss.RegisterAddr,
			Name:       c.Wss.Type,
			Region:     c.Wss.Region,
			MaxConnCnt: c.Wss.MaxConnCnt,
		})
		c.Listen = append(c.Listen, &c.Wss)
	}

}

// 校验参数
func (c *Connd) Validate() error {
	//TODO 验证配置信息
	var count int
	if c.Tcp.Listen == "" {
		count++
	}
	if c.Tls.Listen == "" {
		count++
	}
	if c.Ws.Listen == "" {
		count++
	}
	if c.Wss.Listen != "" {
		count++
	}

	if count == 4 {
		return errors.New("there is no valid address in [tcp,tls,ws,wss]")
	}

	if c.Grpc.Listen == "" {
		return errors.New("there is no valid address in grpc")
	}

	if len(c.Etcd.Cluster) == 0 {
		return errors.New("there is no valid cluster in etcd")
	}
	c.initServer()
	return nil
}
