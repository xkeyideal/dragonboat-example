package raft

import (
	"context"
	"fmt"
)

func (s *Storage) ChangeRaftNodeShardIds(add bool, shardIds []uint64) error {
	set := make(map[uint64]struct{})

	if add {
		for _, shardId := range s.getShardIds() {
			set[shardId] = struct{}{}
		}

		lostShardIds := []uint64{}
		join := make(map[uint64]map[uint64]bool)
		for _, shardId := range shardIds {
			if _, ok := set[shardId]; !ok {
				jn := map[uint64]bool{
					s.cfg.ReplicaId: true,
				}
				join[shardId] = jn
				lostShardIds = append(lostShardIds, shardId)
				set[shardId] = struct{}{}
			}
		}

		return s.stateMachine(join, s.dataDir, lostShardIds)
	}

	for _, shardId := range shardIds {
		set[shardId] = struct{}{}
	}

	for _, shardId := range s.getShardIds() {
		if _, ok := set[shardId]; ok {
			s.mu.Lock()
			delete(s.csMap, shardId)
			delete(s.smMap, shardId)
			s.mu.Unlock()

			set[shardId] = struct{}{}
		}
	}

	return nil
}

// AddRaftNode 添加raft节点，可用于机器的新增或已有机器新增shardIds
func (s *Storage) AddRaftNode(replicaId uint64, target string, shardIds []uint64) error {
	for _, shardId := range shardIds {
		if s.moveToCheck(shardId) {
			if _, err := s.raftNodeInvoke(replicaId, shardId, target, uint32(ADD), false); err != nil {
				return fmt.Errorf("AddRaftNode raftNodeInvoke replicaId:%d, target: %s, shardId: %d, err: %s", replicaId, target, shardId, err.Error())
			}
			continue
		}

		if err := s.addRaftNode(replicaId, target, shardId); err != nil {
			return fmt.Errorf("AddRaftNode localNodeInvoke replicaId:%d, target: %s, shardId: %d, err: %s", replicaId, target, shardId, err.Error())
		}
	}
	return nil
}

func (s *Storage) addRaftNode(replicaId uint64, target string, shardId uint64) error {
	membership, err := s.shardMembership(shardId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestAddNode(ctx, shardId, replicaId, target, membership.ConfigChangeID)
}

// AddRaftObserver 添加raft observer节点，可用于机器的新增或已有机器新增shardIds
func (s *Storage) AddRaftObserver(replicaId uint64, target string, shardIds []uint64) error {
	for _, shardId := range shardIds {
		if s.moveToCheck(shardId) {
			if _, err := s.raftNodeInvoke(replicaId, shardId, target, uint32(OBSERVER), false); err != nil {
				return fmt.Errorf("AddRaftObserver raftNodeInvoke replicaId:%d, target: %s, shardId: %d, err: %s", replicaId, target, shardId, err.Error())
			}
			continue
		}

		if err := s.addRaftObserver(replicaId, target, shardId); err != nil {
			return fmt.Errorf("AddRaftObserver localRaftInvoke replicaId:%d, target: %s, shardId: %d, err: %s", replicaId, target, shardId, err.Error())
		}
	}

	return nil
}

func (s *Storage) addRaftObserver(replicaId uint64, target string, shardId uint64) error {
	membership, err := s.shardMembership(shardId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestAddObserver(ctx, shardId, replicaId, target, membership.ConfigChangeID)
}

// RemoveRaftNodeShards 下线当前Node所在的部分shardIds
func (s *Storage) RemoveRaftNode(replicaId uint64, shardIds []uint64) error {
	for _, shardId := range shardIds {
		if s.moveToCheck(shardId) {
			if _, err := s.raftNodeInvoke(replicaId, shardId, "", uint32(DELETE), false); err != nil {
				return fmt.Errorf("RemoveRaftNode raftNodeInvoke replicaId:%d, shardId: %d, err: %s", replicaId, shardId, err.Error())
			}
			continue
		}

		if err := s.removeRaftNode(replicaId, shardId); err != nil {
			return fmt.Errorf("RemoveRaftNode localRaftInvoke replicaId:%d, shardId: %d, err: %s", replicaId, shardId, err.Error())
		}
	}

	return nil
}

func (s *Storage) removeRaftNode(replicaId, shardId uint64) error {
	membership, err := s.shardMembership(shardId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestDeleteNode(ctx, shardId, replicaId, membership.ConfigChangeID)
}
