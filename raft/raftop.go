package raft

import (
	"context"
	"fmt"
)

func (s *Storage) ChangeRaftNodeClusterIds(add bool, clusterIds []uint64) error {
	set := make(map[uint64]struct{})

	if add {
		for _, clusterId := range s.getClusterIds() {
			set[clusterId] = struct{}{}
		}

		lostClusterIds := []uint64{}
		join := make(map[uint64]map[uint64]bool)
		for _, clusterId := range clusterIds {
			if _, ok := set[clusterId]; !ok {
				jn := map[uint64]bool{
					s.cfg.NodeId: true,
				}
				join[clusterId] = jn
				lostClusterIds = append(lostClusterIds, clusterId)
				set[clusterId] = struct{}{}
			}
		}

		return s.stateMachine(join, s.dataDir, lostClusterIds)
	}

	for _, clusterId := range clusterIds {
		set[clusterId] = struct{}{}
	}

	for _, clusterId := range s.getClusterIds() {
		if _, ok := set[clusterId]; ok {
			s.mu.Lock()
			delete(s.csMap, clusterId)
			delete(s.smMap, clusterId)
			s.mu.Unlock()

			set[clusterId] = struct{}{}
		}
	}

	return nil
}

// AddRaftNode 添加raft节点，可用于机器的新增或已有机器新增clusterIds
func (s *Storage) AddRaftNode(nodeId uint64, target string, clusterIds []uint64) error {
	for _, clusterId := range clusterIds {
		if s.moveToCheck(clusterId) {
			if _, err := s.raftNodeInvoke(nodeId, clusterId, target, uint32(ADD), false); err != nil {
				return fmt.Errorf("AddRaftNode raftNodeInvoke nodeId:%d, target: %s, clusterId: %d, err: %s", nodeId, target, clusterId, err.Error())
			}
			continue
		}

		if err := s.addRaftNode(nodeId, target, clusterId); err != nil {
			return fmt.Errorf("AddRaftNode localNodeInvoke nodeId:%d, target: %s, clusterId: %d, err: %s", nodeId, target, clusterId, err.Error())
		}
	}
	return nil
}

func (s *Storage) addRaftNode(nodeId uint64, target string, clusterId uint64) error {
	membership, err := s.clusterMembership(clusterId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestAddNode(ctx, clusterId, nodeId, target, membership.ConfigChangeID)
}

// AddRaftObserver 添加raft observer节点，可用于机器的新增或已有机器新增clusterIds
func (s *Storage) AddRaftObserver(nodeId uint64, target string, clusterIds []uint64) error {
	for _, clusterId := range clusterIds {
		if s.moveToCheck(clusterId) {
			if _, err := s.raftNodeInvoke(nodeId, clusterId, target, uint32(OBSERVER), false); err != nil {
				return fmt.Errorf("AddRaftObserver raftNodeInvoke nodeId:%d, target: %s, clusterId: %d, err: %s", nodeId, target, clusterId, err.Error())
			}
			continue
		}

		if err := s.addRaftObserver(nodeId, target, clusterId); err != nil {
			return fmt.Errorf("AddRaftObserver localRaftInvoke nodeId:%d, target: %s, clusterId: %d, err: %s", nodeId, target, clusterId, err.Error())
		}
	}

	return nil
}

func (s *Storage) addRaftObserver(nodeId uint64, target string, clusterId uint64) error {
	membership, err := s.clusterMembership(clusterId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestAddObserver(ctx, clusterId, nodeId, target, membership.ConfigChangeID)
}

// RemoveRaftNodeClusters 下线当前Node所在的部分clusterIds
func (s *Storage) RemoveRaftNode(nodeId uint64, clusterIds []uint64) error {
	for _, clusterId := range clusterIds {
		if s.moveToCheck(clusterId) {
			if _, err := s.raftNodeInvoke(nodeId, clusterId, "", uint32(DELETE), false); err != nil {
				return fmt.Errorf("RemoveRaftNode raftNodeInvoke nodeId:%d, clusterId: %d, err: %s", nodeId, clusterId, err.Error())
			}
			continue
		}

		if err := s.removeRaftNode(nodeId, clusterId); err != nil {
			return fmt.Errorf("RemoveRaftNode localRaftInvoke nodeId:%d, clusterId: %d, err: %s", nodeId, clusterId, err.Error())
		}
	}

	return nil
}

func (s *Storage) removeRaftNode(nodeId, clusterId uint64) error {
	membership, err := s.clusterMembership(clusterId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncRequestDeleteNode(ctx, clusterId, nodeId, membership.ConfigChangeID)
}
