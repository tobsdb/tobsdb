package pkg

import (
	"log"
	"os"
)

type LogLevel int

const (
	LogLevelNone LogLevel = iota
	LogLevelErrOnly
	LogLevelDebug
)

var log_level = LogLevelErrOnly

func SetLogLevel(level LogLevel) {
	info_logger.Println("log level set to", level)
	log_level = level
}

var (
	info_logger  = log.New(os.Stdout, "INFO: ", log.LUTC)
	error_logger = log.New(os.Stderr, "ERROR: ", log.LUTC)
	warn_logger  = log.New(os.Stdout, "WARN: ", log.LUTC)
	debug_logger = log.New(os.Stdout, "DEBUG: ", log.LUTC)
)

func InfoLog(message ...any) {
	if log_level >= LogLevelDebug {
		info_logger.Println(message...)
	}
}

func ErrorLog(err ...any) {
	if log_level > LogLevelNone {
		error_logger.Println(err...)
	}
}

func FatalLog(err ...any) {
	if log_level > LogLevelNone {
		error_logger.Fatalln(err...)
	}
}

func WarnLog(message ...any) {
	if log_level >= LogLevelDebug {
		warn_logger.Println(message...)
	}
}

func DebugLog(message ...any) {
	if log_level >= LogLevelDebug {
		debug_logger.Println(message...)
	}
}
