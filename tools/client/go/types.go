package main

import "time"

type (
	TdbInt           int
	TdbString        string
	TdbVector[V any] []V
	TdbFloat         int
	TdbDate          time.Time
	TdbBool          bool
	TdbBytes         []byte
)
