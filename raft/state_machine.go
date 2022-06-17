package raft

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	"github.com/xkeyideal/dragonboat-example/v3/raft/command"
	"github.com/xkeyideal/dragonboat-example/v3/store"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type StateMachine struct {
	ClusterID uint64
	NodeID    uint64
	store     *store.Store

	// db里存储revision的key
	indexKey []byte
}

func newStateMachine(clusterid uint64, nodeId uint64, s *store.Store) *StateMachine {
	// 生成存储revision的key
	smIndexKey := make([]byte, len(indexKeyPrefix)+8)
	copy(smIndexKey, indexKeyPrefix)
	binary.BigEndian.PutUint64(smIndexKey[len(indexKeyPrefix):], clusterid)

	return &StateMachine{
		ClusterID: clusterid,
		NodeID:    nodeId,
		indexKey:  smIndexKey,
		store:     s,
	}
}

func (r *StateMachine) Open(stopChan <-chan struct{}) (uint64, error) {
	select {
	case <-stopChan:
		return 0, sm.ErrOpenStopped
	default:
		cf := r.store.GetColumnFamily(config.ColumnFamilyDefault)
		val, err := r.store.GetBytes(r.store.BuildColumnFamilyKey(cf, r.indexKey))
		if err != nil {
			return 0, err
		}

		// 系统初次启动时，全局revision应该是不存在的，db里查不到，此时返回0
		if len(val) == 0 {
			return 0, nil
		}

		return binary.BigEndian.Uint64(val), nil
	}
}

func (r *StateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	resultEntries := make([]sm.Entry, 0, len(entries))

	//  将raft的日志转换为db要执行的命令
	for _, e := range entries {
		r, err := r.processEntry(e)
		if err != nil {
			return nil, err
		}

		resultEntries = append(resultEntries, r)
	}

	idx := entries[len(entries)-1].Index
	idxByte := make([]byte, 8)
	binary.BigEndian.PutUint64(idxByte, idx)

	batch := r.store.Batch()
	defer batch.Close()

	// 更新revision的值
	cf := r.store.GetColumnFamily(config.ColumnFamilyDefault)
	batch.Set(r.store.BuildColumnFamilyKey(cf, r.indexKey), idxByte, r.store.GetWo())
	if err := r.store.Write(batch); err != nil {
		return nil, err
	}

	return resultEntries, nil
}

func (r *StateMachine) processEntry(e sm.Entry) (sm.Entry, error) {
	cmd, err := command.DecodeCmd(e.Cmd)
	if err != nil {
		return e, err
	}

	opts := &command.WriteOptions{
		Revision: e.Index,
	}

	if err := cmd.LocalInvoke(r.store, opts); err != nil {
		return e, err
	}

	resp := cmd.GetResp()
	e.Result = sm.Result{Value: uint64(len(e.Cmd)), Data: resp}

	return e, nil
}

func (r *StateMachine) Lookup(query interface{}) (interface{}, error) {
	cmd, err := command.DecodeCmd(query.([]byte))
	if err != nil {
		return nil, err
	}

	if err := cmd.LocalInvoke(r.store); err != nil {
		return nil, err
	}

	return cmd.GetResp(), nil
}

func (r *StateMachine) Sync() error {
	return nil
}

func (r *StateMachine) PrepareSnapshot() (interface{}, error) {
	return r.store.NewSnapshotDir()
}

func (r *StateMachine) SaveSnapshot(snapshot interface{}, writer io.Writer, stopChan <-chan struct{}) error {
	path := snapshot.(string)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	return r.store.SaveSnapShotToWriter(path, writer, stopChan)
}

func (r *StateMachine) RecoverFromSnapshot(reader io.Reader, stopChan <-chan struct{}) error {
	return r.store.LoadSnapShotFromReader(reader, stopChan)
}

func (r *StateMachine) Close() error {
	return r.store.Close()
}
