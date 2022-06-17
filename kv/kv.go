package kv

import "github.com/xkeyideal/dragonboat-example/v3/raft"

type KVStorage struct {
	cf          string
	raftStorage *raft.Storage
}

func NewKVStorage(cf string, raftStorage *raft.Storage) *KVStorage {
	return &KVStorage{
		cf:          cf,
		raftStorage: raftStorage,
	}
}

func (kv *KVStorage) Get(key []byte, linearizable bool) (uint64, []byte, error) {
	revision, val, err := kv.raftStorage.Get(kv.cf, key, linearizable, key)
	if err != nil {
		return 0, nil, err
	}

	if len(val) <= 0 {
		return 0, nil, ErrRaftNotFound
	}

	return revision, val, nil
}

func (kv *KVStorage) Put(key, val []byte) (uint64, error) {
	return kv.raftStorage.Put(kv.cf, key, key, val)
}

func (kv *KVStorage) Del(key []byte) error {
	return kv.raftStorage.Del(kv.cf, key, key)
}
