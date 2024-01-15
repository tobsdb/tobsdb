package main

import "fmt"

func Log(level LogLevel, msg string) {
	fmt.Printf("[TOBSDB:%s] %s\n", level, msg)
}

type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
	LogLevelWarn  LogLevel = "WARN"
)
