package config

import (
	"errors"
	"log"
	"math"

	"github.com/xkeyideal/dragonboat-example/v3/tools"
	"go.uber.org/zap/zapcore"
)

const (
	// 系统使用的
	ColumnFamilyDefault = "default"

	LogDir  = "/tmp/logs"
	RaftDir = "/tmp/raft"

	Serving = 1 // 系统提供服务
	Abort   = 2 // 系统尚未准备好
)

var (
	PebbleColumnFamilyMap = map[string]byte{
		ColumnFamilyDefault: 1,
	}

	ErrSystemAborted = errors.New("system raft not ready")
)

type SystemConfig struct {
	KeepAliveMinTime     int
	WriteBufferSize      int
	ReadBufferSize       int
	MaxRecvMsgSize       int
	MaxSendMsgSize       int
	MaxConcurrentStreams uint32

	// 本机的地址
	IP string

	// grpc port
	GrpcPort uint16

	// zap log相关配置项
	LogDir   string
	LogLevel zapcore.Level

	// metrics addr
	MetricsAddr string
}

func NewSystemConfig() *SystemConfig {
	ip, err := tools.GetIP("auto")
	if err != nil {
		panic(err)
	}

	cfg := &SystemConfig{
		KeepAliveMinTime:     60,
		WriteBufferSize:      5 * 1024, // 5KB
		ReadBufferSize:       5 * 1024,
		MaxRecvMsgSize:       4 * 1024 * 1024, // grpc默认值为4MB
		MaxSendMsgSize:       math.MaxInt32,   // 使用grpc默认值
		MaxConcurrentStreams: math.MaxUint32,
		GrpcPort:             12345,
		IP:                   ip,
		LogLevel:             zapcore.WarnLevel,
		LogDir:               LogDir,
		MetricsAddr:          "",
	}

	log.Println("[INFO] log storage filepath:", LogDir)
	log.Println("[INFO] raft storage filepath:", RaftDir)
	log.Println("[INFO] dragonboat metrics address:", cfg.MetricsAddr)

	return cfg
}
