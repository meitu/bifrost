package log

import (
	"fmt"
	"io"
	"net/http"
	"os"
	ospath "path"
	"time"

	rolling "github.com/arthurkiller/rollingwriter"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func CreateLogger(path, level, pattern, name, format, url string, compress bool) (*zap.Logger, error) {
	writer, err := Writer(path, pattern, compress)
	if err != nil {
		return nil, fmt.Errorf("create IOWriter failed, %s", err)
	}

	logger, err := Logger(writer, level, name, format, url)
	if err != nil {
		return nil, fmt.Errorf("init log failed, %s", err)
	}
	return logger, nil
}

//Writer generate the rollingWriter
func Writer(path, pattern string, compress bool) (io.Writer, error) {
	var opts []rolling.Option
	opts = append(opts, rolling.WithRollingTimePattern(pattern))
	if compress {
		opts = append(opts, rolling.WithCompress())
	}
	dir, filename := ospath.Split(path)
	opts = append(opts, rolling.WithLogPath(dir), rolling.WithFileName(filename), rolling.WithLock())
	writer, err := rolling.NewWriter(opts...)
	if err != nil {
		return nil, fmt.Errorf("create IOWriter failed, %s", err)
	}
	return writer, nil
}

func Logger(w io.Writer, level, name, format, url string) (*zap.Logger, error) {
	lv := zap.NewAtomicLevel()
	switch level {
	case "debug":
		lv.SetLevel(zap.DebugLevel)
	case "info":
		lv.SetLevel(zap.InfoLevel)
	case "warn":
		lv.SetLevel(zap.WarnLevel)
	case "error":
		lv.SetLevel(zap.ErrorLevel)
	case "panic":
		lv.SetLevel(zap.PanicLevel)
	case "fatal":
		lv.SetLevel(zap.FatalLevel)
	default:
		return nil, fmt.Errorf("unknown log level(%s)\n", level)
	}

	timeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Local().Format(format))
	}

	encoderCfg := zapcore.EncoderConfig{
		NameKey:        name,
		StacktraceKey:  "stack",
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		CallerKey:      "caller",
		EncodeTime:     timeEncoder,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	http.Handle(url, lv)
	encoder := zapcore.NewJSONEncoder(encoderCfg)
	output := zapcore.AddSync(w)
	log := zap.New(zapcore.NewCore(encoder, output, lv), zap.AddCaller())

	return log.With(zap.Int("pid", os.Getpid())), nil
}

func CreateLogrus(path, level, pattern string, compress bool) error {
	writer, err := Writer(path, pattern, compress)
	if err != nil {
		return fmt.Errorf("create IOWriter failed, %s", err)
	}

	logrus.SetOutput(writer)
	switch level {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	default:
		return fmt.Errorf("unknown log level(%s)\n", level)
	}
	return nil
}
