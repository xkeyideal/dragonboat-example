package ilogger

import (
	"fmt"
	"path/filepath"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	zlog "github.com/xkeyideal/dragonboat-example/v3/internal/logger"

	"github.com/lni/dragonboat/v3/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerOptions struct {
	logDir    string
	replicaId uint64
	target    string
}

var Lo *LoggerOptions = &LoggerOptions{
	logDir: config.LogDir,
}

func (lo *LoggerOptions) SetLogDir(dir string) {
	lo.logDir = dir
}

func (lo *LoggerOptions) SetReplicaId(replicaId uint64) {
	lo.replicaId = replicaId
}

func (lo *LoggerOptions) SetTarget(target string) {
	lo.target = target
}

func init() {
	logger.SetLoggerFactory(RaftFactory)
	logger.GetLogger("raft").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("gossip").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.ERROR)
	logger.GetLogger("raftpb").SetLevel(logger.ERROR)
	logger.GetLogger("config").SetLevel(logger.ERROR)
	logger.GetLogger("settings").SetLevel(logger.INFO)
}

type raftLogger struct {
	pkgName string
	logDir  string
	log     *zap.Logger
}

func NewRaftLogger(logDir, pkgName string, level zapcore.Level) *raftLogger {
	name := fmt.Sprintf("%s.log", pkgName)
	return &raftLogger{
		pkgName: pkgName,
		logDir:  logDir,
		log:     zlog.NewLogger(filepath.Join(logDir, name), level, false),
	}
}

func RaftFactory(pkgName string) logger.ILogger {
	return &raftLogger{
		logDir:  Lo.logDir,
		pkgName: pkgName,
	}
}

var _ logger.ILogger = (*raftLogger)(nil)

func (c *raftLogger) SetLevel(level logger.LogLevel) {
	var cl zapcore.Level
	if level == logger.CRITICAL {
		cl = zapcore.PanicLevel
	} else if level == logger.ERROR {
		cl = zapcore.ErrorLevel
	} else if level == logger.WARNING {
		cl = zapcore.WarnLevel
	} else if level == logger.INFO {
		cl = zapcore.InfoLevel
	} else if level == logger.DEBUG {
		cl = zapcore.DebugLevel
	} else {
		panic("unexpected level")
	}

	name := fmt.Sprintf("dragonboat-%s.log", c.pkgName)
	c.log = zlog.NewLogger(filepath.Join(c.logDir, name), cl, false)
}

func (c *raftLogger) fmsg() string {
	return "[multiraft] [" + c.pkgName + "]"
}

func (c *raftLogger) Debugf(format string, args ...interface{}) {
	c.log.Debug(c.fmsg(),
		zap.String("target", Lo.target),
		zap.Uint64("replicaId", Lo.replicaId),
		zap.String("msg", fmt.Sprintf(format, args...)),
	)
}

func (c *raftLogger) Infof(format string, args ...interface{}) {
	c.log.Info(c.fmsg(), zap.String("target", Lo.target),
		zap.Uint64("replicaId", Lo.replicaId),
		zap.String("msg", fmt.Sprintf(format, args...)),
	)
}

func (c *raftLogger) Warningf(format string, args ...interface{}) {
	c.log.Warn(c.fmsg(),
		zap.String("target", Lo.target),
		zap.Uint64("replicaId", Lo.replicaId),
		zap.String("msg", fmt.Sprintf(format, args...)),
	)
}

func (c *raftLogger) Errorf(format string, args ...interface{}) {
	c.log.Error(c.fmsg(),
		zap.String("target", Lo.target),
		zap.Uint64("replicaId", Lo.replicaId),
		zap.String("msg", fmt.Sprintf(format, args...)),
	)
}

func (c *raftLogger) Panicf(format string, args ...interface{}) {
	c.log.Panic(c.fmsg(),
		zap.String("target", Lo.target),
		zap.Uint64("replicaId", Lo.replicaId),
		zap.String("msg", fmt.Sprintf(format, args...)),
	)
}
