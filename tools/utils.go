package tools

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io/ioutil"
)

func GZipEncode(content []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(content); err != nil {
		return nil, err
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func GZipDecode(buf []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

func IntToBytes(i interface{}) []byte {
	switch i.(type) {
	case int64:
		var buf = make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i.(int64)))
		return buf
	case uint64:
		var buf = make([]byte, 8)
		binary.BigEndian.PutUint64(buf, i.(uint64))
		return buf
	case int32:
		var buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(i.(int32)))
		return buf
	case uint32:
		var buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf, i.(uint32))
		return buf
	case int16:
		var buf = make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(i.(int16)))
		return buf
	case uint16:
		var buf = make([]byte, 2)
		binary.BigEndian.PutUint16(buf, i.(uint16))
		return buf
	}

	return []byte{}
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func BytesToUInt64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func BytesToInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}
