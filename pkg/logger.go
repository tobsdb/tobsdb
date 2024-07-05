package pkg

import (
	"io"
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

	switch level {
	case LogLevelNone:
		info_logger.SetOutput(io.Discard)
		error_logger.SetOutput(io.Discard)
		fatal_logger.SetOutput(io.Discard)
		warn_logger.SetOutput(io.Discard)
		debug_logger.SetOutput(io.Discard)
	case LogLevelErrOnly:
		error_logger.SetOutput(os.Stderr)
		fatal_logger.SetOutput(os.Stderr)

		info_logger.SetOutput(io.Discard)
		warn_logger.SetOutput(io.Discard)
		debug_logger.SetOutput(io.Discard)
	case LogLevelDebug:
		error_logger.SetOutput(os.Stderr)
		fatal_logger.SetOutput(os.Stderr)

		info_logger.SetOutput(os.Stdout)
		warn_logger.SetOutput(os.Stdout)
		debug_logger.SetOutput(os.Stdout)
	}
}

var (
	info_logger  = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)
	error_logger = log.New(os.Stderr, "ERROR: ", log.Lshortfile|log.LstdFlags)
	fatal_logger = log.New(os.Stderr, "FATAL: ", log.Lshortfile|log.LstdFlags)
	warn_logger  = log.New(os.Stdout, "WARN: ", log.Lshortfile|log.LstdFlags)
	debug_logger = log.New(os.Stdout, "DEBUG: ", log.Lshortfile|log.LstdFlags)
)

var (
	InfoLog  = info_logger.Println
	ErrorLog = error_logger.Println
	FatalLog = fatal_logger.Fatalln
	WarnLog  = warn_logger.Println
	DebugLog = debug_logger.Println
)
