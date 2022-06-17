package config

import "errors"

const (
	// 系统使用的
	ColumnFamilyDefault = "default"

	// 存储使用
	ColumnFamilyKV = "sidecar"

	LogDir = "/tmp/logs"
)

var (
	PebbleColumnFamilyMap = map[string]byte{
		ColumnFamilyDefault: 1,
		ColumnFamilyKV:      2,
	}

	ErrSystemAborted = errors.New(" system raft not ready")
)
