package conf

import (
	"encoding/json"
	"errors"
	"time"
)

type Pushd struct {
	PIDFileName string `cfg:"pid-filename; pushd.pid; ; the file name to record pushd PID"`
	Logger      Logger `cfg:"logger"`
	Status      Status `cfg:"status"`
	Server      Server `cfg:"server"`
}

type Server struct {
	Servname string   `cfg:"servname;pushd; ; default callback service name and namepace tikv"`
	Callback Callback `cfg:"callback"`
	Connd    Connd    `cfg:"connd"`
	Store    Store    `cfg:"store"`
	Publish  Publish  `cfg:"publish"`
	Push     Push     `cfg:"pushd"`
	Etcd     Etcd     `cfg:"etcd"`

	Region       string        `cfg:"region; region;nonempty ; the region name of pushd"`
	Capacity     int64         `cfg:"cap; 1000; numeric; the capacity of token"`
	FillInterval time.Duration `cfg:"fill-interval; 100ms; ; the interval for issuing tokens in millisecond"`
	Quantum      int64         `cfg:"quantum; 100; numeric; the quantity of token to issue every interval"`
	MaxWait      time.Duration `cfg:"max-wait; 0ms; ; wait for this duration if failed to get token in millisecond"`
	Listen       string        `cfg:"listen;127.0.0.1:5053; netaddr; address to listen"`
}

type GC struct {
	Enable         bool          `cfg:"enable;true;boolean;enable store gc "`
	BatchLimit     int64         `cfg:"batch-limit;256; nonempty;gc batch limit number"`
	Interval       time.Duration `cfg:"interval;1s; nonempty;gc interval"`
	LeaderLifeTime time.Duration `cfg:"leader-life-time;1m0s; nonempty;gc leader life time"`
}

type Logger struct {
	DebugLog  DebugLogger `cfg:"debug-logger"`
	AccessLog DebugLogger `cfg:"access-logger"`
	TikvLog   TikvLogger  `cfg:"tikv-logger"`
}

type DebugLogger struct {
	Name       string `cfg:"name; pushd_debug; ; the default logger name"`
	Path       string `cfg:"path; logs/pushd; ; the default log path"`
	Format     string `cfg:"format;2006-01-02 15:04:05.999999999; ; log time format"`
	Level      string `cfg:"level; info; ; log level(debug, info, warn, error, panic, fatal)"`
	TimeRotate string `cfg:"time-rotate; 0 0 0 * * *; ; log time rotate pattern(s m h D M W)"`
	Compress   bool   `cfg:"compress; false; boolean; true for enabling log compress"`
}

type TikvLogger struct {
	Path       string `cfg:"path; logs/tikv;nonempty ; the default log path"`
	Level      string `cfg:"level; info; ; log level(debug, info, warn, error, panic, fatal)"`
	TimeRotate string `cfg:"time-rotate; 0 0 0 * * *; ; log time rotate pattern(s m h D M W)"`
	Compress   bool   `cfg:"compress; false; boolean; true for enabling log compress"`
}

type Publish struct {
	Servname string `cfg:"-"`

	RegisterAddr string        `cfg:"register-addr; ; ; address to register into ETCD"`
	Service      string        `cfg:"service; publish;nonempty ; service name"`
	Group        string        `cfg:"group; group;nonempty ; group"`
	Auth         string        `cfg:"auth;"";;client connetion auth"`
	GrpcSlowlog  time.Duration `cfg:"grpc-slowlog; 100ms; ; grpc slowlog threshold in millisecond"`
}

type Push struct {
	Servname string `cfg:"-"`

	Service      string        `cfg:"service; pushd;nonempty ; service name"`
	Group        string        `cfg:"group; group;nonempty ; group"`
	RegisterAddr string        `cfg:"register-addr; ; ; address to register into ETCD"`
	GrpcSlowlog  time.Duration `cfg:"grpc-slowlog; 100ms; ; grpc slowlog threshold in millisecond"`
}

type Store struct {
	// BizCtrlMID      bool          `cfg:"biz-ctrl-mid;false;boolean; business control id generation"`
	Name            string        `cfg:"name;tikv;;store name"`
	Redis           RedisStore    `cfg:"redis-store"`
	Tikv            Tikv          `cfg:"tikv"`
	Slowlog         time.Duration `cfg:"slowlog; 100ms; ;slowlog threshold in millisecond"`
	SessionRecovery bool          `cfg:"sessionrecovery; true; boolean; support session recovery or not"`
}

type RedisStore struct {
	Queue Queue `cfg:"queue"`
	Redis Redis `cfg:"redis"`
}

type Tikv struct {
	Addrs  string `cfg:"pb-addrs;tikv://172.16.201.74:2379; nonempty;pb address in tidb"`
	GC     GC     `cfg:"gc"`
	TikvGC TikvGC `cfg:"tikv-gc"`
}

type TikvGC struct {
	Enable            bool          `cfg:"enable;true;boolean;enable tikv gc "`
	Interval          time.Duration `cfg:"interval;20m;;gc work tick interval"`
	LeaderLifeTime    time.Duration `cfg:"leader-life-time;30m;;lease flush leader interval"`
	SafePointLifeTime time.Duration `cfg:"safe-point-life-time;10m;;safe point life time "`
	Concurrency       int           `cfg:"concurrency;2;;gc work concurrency"`
}

type Callback struct {
	Service           string `cfg:"service; callback;nonempty ; service name"`
	Region            string `cfg:"region; region;nonempty ; region"`
	AppKey            string `cfg:"appkey; appkey; ; application key"`
	MinProviderNodes  uint32 `cfg:"min-provider-nodes; 3; numeric; the min number of callback nodes"`
	LoadBalancingMode string `cfg:"load-balancing-mode;hash;;hash method load balancing to the client of callback"`
	Etcd              *Etcd
}

type Connd struct {
	Client Client `cfg:"client"`
}

type Client struct {
	NotifyChanSize  int           `cfg:"notify-chan-size;1024; >=1024 ;the size of channel which used for notify"`
	WorkerProcesses int           `cfg:"worker-processes;10; >= 3 ;number of worker goroutines to process notifications"`
	RetryTimes      int           `cfg:"retry-times; 3; >0;call the pushd grpc retry times"`
	DefaultTimeout  time.Duration `cfg:"default-timeout; 600ms; ; default time out"`
	Slowlog         time.Duration `cfg:"slowlog; 100ms; ; grpc slowlog threshold in millisecond"`
}

type Etcd struct {
	Cluster  []string `cfg:"cluster;['http://127.0.0.1:2379']; nonempty; etcd cluster endpoints"`
	Username string   `cfg:"username;;; username of etcd"`
	Password string   `cfg:"password;;; password of etcd"`
}

//Status is a http server that export server status and metrics
//expvar, grpctrace, prometheus
type Status struct {
	Listen string `cfg:"listen;0.0.0.0:22345;nonempty; listen address of http server"`
	Enable bool   `cfg:"enable; false; boolean; enable opentracing"`
}

type Redis struct {
	Masters          []string      `cfg:"master; ['redis://127.0.0.1:6379']; url; master cluster"`
	Slaves           [][]string    `cfg:"slaves; [['redis://127.0.0.1:6379']]; url; all slave clusters"`
	Auth             string        `cfg:"auth;;; password to connect to redis"`
	Timeout          int64         `cfg:"timeout; 5000; >10; timeout of redis request in milliseconds"`
	MaxIdle          int           `cfg:"maxidle; 500; <10000; max number of idle redis connections"`
	MaxActive        int           `cfg:"maxactive; 0; >=0; max number of connections, 0 means no limited"`
	IdleTimeout      int64         `cfg:"idletimeout; 240000; >=0; close connections after remaining idle for this duration, should be less than the server's timeout, when 0, no connection will be closed"`
	Wait             bool          `cfg:"wait; true; boolean; block Get if exceeds the MaxActive"`
	SlowlogThreshold time.Duration `cfg:"slowlog; 200ms; ;slowlog threshold for redis option"`
}

type Queue struct {
	Redis           *Redis `cfg:"-"`
	Size            uint64 `cfg:"size; 1000; numeric; the max size of queue"`
	AbortOnOverflow bool   `cfg:"abort-overflow; false; boolean; abort connection when topic queue is overflow"`
	Expire          uint64 `cfg:"expire; 20; numeric;  retain message expire time"`
}

func (pushd *Pushd) String() string {
	// Copy the object and hide redis auth value in case it is logged or published(/debug/vars)
	ss := *pushd
	data, err := json.Marshal(ss)
	if err != nil {
		return ""
	}
	return string(data)
}

func (pushd *Pushd) initServer() {
	pushd.Server.Callback.Etcd = &pushd.Server.Etcd
	if pushd.Server.Publish.RegisterAddr == "" {
		pushd.Server.Publish.RegisterAddr = pushd.Server.Listen
	}
	if pushd.Server.Push.RegisterAddr == "" {
		pushd.Server.Push.RegisterAddr = pushd.Server.Listen
	}
	pushd.Server.Publish.Servname = pushd.Server.Servname
	pushd.Server.Push.Servname = pushd.Server.Servname

}

func (pushd *Pushd) ValidateConf() error {
	if len(pushd.Server.Listen) == 0 {
		return errors.New("publish listen is null")
	}
	if len(pushd.Status.Listen) == 0 {
		return errors.New("status listen is null")
	}
	pushd.initServer()
	return nil
}
