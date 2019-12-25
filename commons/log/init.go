package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// globalLog can used to print the log directly
var globalLog *zap.Logger

func init() {
	globalLog = zap.L()
}

func GlobalLogger() *zap.Logger {
	return globalLog
}

func SetGlobalLogger(log *zap.Logger) {
	globalLog = log
}

// Debug can print out the log directly with the globalLog
func Debug(msg string, fields ...zap.Field) {
	globalLog.Debug(msg, fields...)
}

// Info can print out the log directly with the globalLog
func Info(msg string, fields ...zap.Field) {
	globalLog.Info(msg, fields...)
}

// Warn can print out the log directly with the globalLog
func Warn(msg string, fields ...zap.Field) {
	globalLog.Warn(msg, fields...)
}

// Error can print out the log directly with the globalLog
func Error(msg string, fields ...zap.Field) {
	globalLog.Error(msg, fields...)
}

// Panic can print out the log directly with the globalLog
func Panic(msg string, fields ...zap.Field) {
	globalLog.Panic(msg, fields...)
}

// Fatal can print out the log directly with the globalLog
func Fatal(msg string, fields ...zap.Field) {
	globalLog.Fatal(msg, fields...)
}

func Sugar() *zap.SugaredLogger {
	return globalLog.Sugar()
}

//Check returns a CheckedEntry if logging a message at the specified level is enabled
func Check(lvl zapcore.Level, msg string) *zapcore.CheckedEntry {
	return globalLog.Check(lvl, msg)
}
