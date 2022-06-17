package config

import "errors"

const (
	// 系统使用的
	ColumnFamilyDefault = "default"

	LogDir = "/tmp/logs"
)

var (
	PebbleColumnFamilyMap = map[string]byte{
		ColumnFamilyDefault: 1,
	}

	ErrSystemAborted = errors.New("system raft not ready")
)
