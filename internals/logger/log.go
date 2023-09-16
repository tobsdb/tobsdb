package logger

import "log"

func RequestLog(method, table string) {
	log.Printf("%s request on table %s", method, table)
}

func ErrorLog(method, table, err string) {
	log.Printf("%s error on table %s: %s", method, table, err)
}

func SuccessLog(method, table string) {
	log.Printf("%s success on table %s", method, table)
}

func InfoLog(method, table, msg string) {
	log.Printf("%s info on table %s: %s", method, table, msg)
}
