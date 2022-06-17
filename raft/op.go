package raft

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/raft/command"
)

func (s *Storage) Put(cfName string, hashKey []byte, key, val []byte) (uint64, error) {
	cmd := command.NewPutCommand(cfName, key, val)

	shardId := s.getShardId(hashKey)
	var (
		res []byte
		err error
	)

	if s.moveToCheck(shardId) {
		res, err = s.moveToInvoke(shardId, cmd)
	} else {
		res, err = s.invoke(shardId, cmd)
	}

	if err != nil {
		return 0, err
	}

	if len(res) == 0 {
		return 0, errors.New("storage can not find revision")
	}

	return binary.BigEndian.Uint64(res), nil
}

func (s *Storage) Get(cfName string, hashKey []byte, linearizable bool, key []byte) (uint64, []byte, error) {
	cmd := command.NewGetCommand(cfName, key, linearizable)
	shardId := s.getShardId(hashKey)

	var (
		res []byte
		err error
	)

	if s.moveToCheck(shardId) {
		res, err = s.moveToInvoke(shardId, cmd)
	} else {
		res, err = s.invoke(shardId, cmd)
	}

	if err != nil {
		return 0, nil, err
	}

	return binary.BigEndian.Uint64(res), res[8:], nil
}

func (s *Storage) TryLock(lockTimeout uint64, cfName string, key []byte) (bool, error) {
	var currentUnix = uint64(time.Now().Unix())

	var timeoutSecond = currentUnix + lockTimeout
	if timeoutSecond <= currentUnix {
		return false, nil
	}
	cmd := command.NewTryLockCommand(cfName, key, timeoutSecond)
	shardId := s.getShardId(key)
	var (
		res []byte
		err error
	)

	if s.moveToCheck(shardId) {
		res, err = s.moveToInvoke(shardId, cmd)
	} else {
		res, err = s.invoke(shardId, cmd)
	}

	if err != nil {
		return false, err
	}

	if len(res) > 0 && res[0] == 1 {
		return true, nil
	}
	return false, nil
}

func (s *Storage) TryUnLock(cfName string, key []byte) (bool, error) {
	cmd := command.NewTryUnLockCommand(cfName, key)
	shardId := s.getShardId(key)
	var (
		res []byte
		err error
	)

	if s.moveToCheck(shardId) {
		res, err = s.moveToInvoke(shardId, cmd)
	} else {
		res, err = s.invoke(shardId, cmd)
	}

	if err != nil {
		return false, err
	}

	if len(res) > 0 && res[0] == 1 {
		return true, nil
	}
	return false, nil
}

func (s *Storage) Search(cfName string, hashKey []byte, linearizable bool, prefix []byte) ([][]byte, error) {
	cmd := command.NewSearchCommand(cfName, prefix, linearizable)
	shardId := s.getShardId(hashKey)

	var (
		res []byte
		err error
	)

	if s.moveToCheck(shardId) {
		res, err = s.moveToInvoke(shardId, cmd)
	} else {
		res, err = s.invoke(shardId, cmd)
	}

	if err != nil {
		return nil, err
	}

	result := [][]byte{}
	if res != nil {
		command.Decode(res, &result)
	}
	return result, nil
}

func (s *Storage) Del(cfName string, hashKey, key []byte) error {
	cmd := command.NewDelCommand(cfName, key)
	shardId := s.getShardId(hashKey)

	var (
		err error
	)

	if s.moveToCheck(shardId) {
		_, err = s.moveToInvoke(shardId, cmd)
	} else {
		_, err = s.invoke(shardId, cmd)
	}

	return err
}

func (s *Storage) DelPrefix(cfName string, hashKey, prefix []byte) error {
	cmd := command.NewDelPrefixCommand(cfName, prefix)
	shardId := s.getShardId(hashKey)

	var (
		err error
	)

	if s.moveToCheck(shardId) {
		_, err = s.moveToInvoke(shardId, cmd)
	} else {
		_, err = s.invoke(shardId, cmd)
	}

	return err
}
