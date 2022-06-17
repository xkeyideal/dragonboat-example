package pebbledb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	"github.com/xkeyideal/dragonboat-example/v3/raft/command"
	"github.com/xkeyideal/dragonboat-example/v3/store"

	"go.uber.org/atomic"
)

func initPebble() *store.Store {
	dir := "/tmp/pebbledb"
	nodeId := 200000
	clusterId := 10001
	raftPath := filepath.Join(dir, fmt.Sprintf("data_node%d", nodeId))
	s, err := store.NewStore(uint64(clusterId), filepath.Join(raftPath, strconv.Itoa(int(clusterId))), store.PebbleClusterOption{}, nil)
	if err != nil {
		panic(err)
	}

	return s
}

func TestPebbleGetPut(t *testing.T) {
	s := initPebble()

	cfName := config.ColumnFamilyDefault
	revision := 100
	key := []byte("hello")
	val := []byte("world")

	cmd := command.NewPutCommand(cfName, key, val)
	opts := &command.WriteOptions{
		Revision: uint64(revision),
	}
	err := cmd.LocalInvoke(s, opts)
	if err != nil {
		t.Fatal(err)
	}

	cmd2 := command.NewGetCommand(cfName, key, false)
	err = cmd2.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res := cmd2.GetResp()
	expectedRevision, expectedVal := binary.BigEndian.Uint64(res), res[8:]
	if bytes.Equal(expectedVal, val) && expectedRevision == uint64(revision) {

	} else {
		t.Fatalf("unexpected get response, %d,%d, expected: %s, actual: %s|", revision, expectedRevision, string(val), string(expectedVal))
	}

	s.Close()
}

func TestPebbleSeek(t *testing.T) {
	s := initPebble()
	cfName := config.ColumnFamilyDefault

	revision := 100
	prefix := []byte("hell")
	for i := 0; i < 10; i++ {
		key := append(prefix, byte('a'+i))
		cmd := command.NewPutCommand(cfName, key, key)
		opts := &command.WriteOptions{
			Revision: uint64(revision),
		}
		err := cmd.LocalInvoke(s, opts)
		if err != nil {
			t.Fatal(err)
		}

		revision += 1
	}

	cmd2 := command.NewGetCommand(cfName, []byte("helld"), false)
	err := cmd2.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res := cmd2.GetResp()
	expectedRevision, expectedVal := binary.BigEndian.Uint64(res), res[8:]
	t.Logf("helld value: %d, %s", expectedRevision, string(expectedVal))

	cmd := command.NewSearchCommand(cfName, prefix, false)
	err = cmd.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res = cmd.GetResp()
	result := [][]byte{}
	if res != nil {
		command.Decode(res, &result)
	}

	for i := 0; i < len(result); i += 2 {
		t.Log(string(result[i]), string(result[i+1]))
	}

	s.Close()
}

func TestPebbleDelete(t *testing.T) {
	s := initPebble()

	cfName := config.ColumnFamilyDefault
	revision := 100
	key := []byte("del_hello")
	val := []byte("world")

	cmd := command.NewPutCommand(cfName, key, val)
	opts := &command.WriteOptions{
		Revision: uint64(revision),
	}
	err := cmd.LocalInvoke(s, opts)
	if err != nil {
		t.Fatal(err)
	}

	cmd2 := command.NewGetCommand(cfName, key, false)
	err = cmd2.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res := cmd2.GetResp()
	expectedRevision, expectedVal := binary.BigEndian.Uint64(res), res[8:]
	if bytes.Equal(expectedVal, val) && expectedRevision == uint64(revision) {

	} else {
		t.Fatalf("unexpected get response, %d,%d, expected: %s, actual: %s|", revision, expectedRevision, string(val), string(expectedVal))
	}

	cmd3 := command.NewDelCommand(cfName, key)
	err = cmd3.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	cmd2 = command.NewGetCommand(cfName, key, false)
	err = cmd2.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res = cmd2.GetResp()

	if len(res) != 0 {
		t.Fatal("del key failed")
	}

	s.Close()
}

func TestPebbleDelPrefix(t *testing.T) {
	s := initPebble()
	defer s.Close()

	cfName := config.ColumnFamilyDefault

	revision := 100
	prefix := []byte("del_prefix_hell")
	for i := 0; i < 10; i++ {
		key := append(prefix, byte('a'+i))
		cmd := command.NewPutCommand(cfName, key, key)
		opts := &command.WriteOptions{
			Revision: uint64(revision),
		}
		err := cmd.LocalInvoke(s, opts)
		if err != nil {
			t.Fatal(err)
		}

		revision += 1
	}

	cmd2 := command.NewGetCommand(cfName, []byte("del_prefix_helld"), false)
	err := cmd2.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res := cmd2.GetResp()
	expectedRevision, expectedVal := binary.BigEndian.Uint64(res), res[8:]
	t.Logf("helld value: %d, %s", expectedRevision, string(expectedVal))

	cmd := command.NewDelPrefixCommand(cfName, prefix)
	err = cmd.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	cmd3 := command.NewSearchCommand(cfName, prefix, false)
	err = cmd.LocalInvoke(s)
	if err != nil {
		t.Fatal(err)
	}

	res = cmd3.GetResp()
	result := [][]byte{}
	if res != nil {
		command.Decode(res, &result)
	}

	if len(result) != 0 {
		t.Fatal("del prefix failed")
	}
}

func TestPebbleLock(t *testing.T) {
	s := initPebble()
	defer s.Close()

	confID := "lock_test"
	var sss atomic.Int64
	var startTime = time.Now()
	var group sync.WaitGroup
	group.Add(15)

	for i := 0; i < 15; i++ {
		go func() {
			for {
				var currentTime = time.Now()
				if currentTime.Unix()-startTime.Unix() > 60 {
					group.Done()
					return
				}
				ok, err := lock(confID, s)
				if err != nil {
					panic(err)
				}
				if ok {
					sss.Inc()
					if ok, err := unlock(confID, s); ok == false || err != nil {
						panic(err)
					}
					sss.Dec()
				}
			}
		}()
	}

	group.Wait()
	if sss.Load() != 0 {
		t.Fatal("lock error")
	}
}

func lock(confID string, s *store.Store) (bool, error) {
	cfName := config.ColumnFamilyDefault
	cmd := command.NewTryLockCommand(cfName, confID, 4)
	err := cmd.LocalInvoke(s)
	if err != nil {
		return false, err
	}

	res := cmd.GetResp()
	if len(res) > 0 && res[0] == 1 {
		return true, nil
	}
	return false, nil
}

func unlock(confID string, s *store.Store) (bool, error) {
	cfName := config.ColumnFamilyDefault
	cmd := command.NewTryUnLockCommand(cfName, confID)
	err := cmd.LocalInvoke(s)
	if err != nil {
		return false, err
	}

	res := cmd.GetResp()
	if len(res) > 0 && res[0] == 1 {
		return true, nil
	}
	return false, nil
}
