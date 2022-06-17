package kv

import (
	"errors"
	"sort"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	pb "github.com/xkeyideal/dragonboat-example/v3/pb/api"
	"github.com/xkeyideal/dragonboat-example/v3/raft"
)

const (
	sep = "$^$"
)

var (
	ErrRaftNotFound = errors.New("raft pebbledb not found")
)

type RaftMeta struct {
	cf          string
	raftStorage *raft.Storage
}

func NewRaftMeta(cf string, raftStorage *raft.Storage) *RaftMeta {
	return &RaftMeta{
		cf:          cf,
		raftStorage: raftStorage,
	}
}

func (rm *RaftMeta) AddRaftObserverNode(replicaId uint64, addr string, shardIds []uint64) error {
	return rm.raftStorage.AddRaftObserver(replicaId, addr, shardIds)
}

func (rm *RaftMeta) AddRaftNode(replicaId uint64, target string, shardIds []uint64) error {
	return rm.raftStorage.AddRaftNode(replicaId, target, shardIds)
}

func (rm *RaftMeta) RemoveRaftNode(replicaId uint64, shardIds []uint64) error {
	return rm.raftStorage.RemoveRaftNode(replicaId, shardIds)
}

func (rm *RaftMeta) Lock(key []byte, timeout uint64) (bool, error) {
	return rm.raftStorage.TryLock(timeout, rm.cf, key)
}

func (rm *RaftMeta) Unlock(key []byte) error {
	_, err := rm.raftStorage.TryUnLock(rm.cf, key)
	return err
}

func (rm *RaftMeta) GetNodeHost() []string {
	return rm.raftStorage.GetNodeHost()
}

func (rm *RaftMeta) UpdateShardMessage(cm *gossip.RaftShardMessage) {
	rm.raftStorage.UpdateShardMessage(cm)
}

func (rm *RaftMeta) UpdateRaftShardIds(add bool, shardIds []uint64) error {
	return rm.raftStorage.ChangeRaftNodeShardIds(add, shardIds)
}

func (rm *RaftMeta) RaftShardInfo() (*pb.RaftInfoResponse, error) {
	memberInfos := rm.raftStorage.GetLocalMembership()
	shardInfos := rm.raftStorage.GetShardMessage()
	gossipInfos := rm.raftStorage.GetAliveInstances()

	resp := &pb.RaftInfoResponse{
		MemberInfos: []*pb.RaftInfoResponse_MemberInfo{},
		ShardInfos: &pb.RaftInfoResponse_ShardInfo{
			Revision: shardInfos.Revision,
			Targets:  make(map[string]*pb.TargetShard),
			Shards:   make(map[uint64]*pb.RaftInfoResponse_Target),
		},
		GossipInfos: gossipInfos,
	}

	for _, info := range memberInfos {
		resp.MemberInfos = append(resp.MemberInfos, &pb.RaftInfoResponse_MemberInfo{
			ShardId:        info.ShardId,
			ConfigChangeId: info.ConfigChangeId,
			Replicas:       info.Replicas,
			LeaderId:       info.LeaderId,
			LeaderValid:    info.LeaderValid,
			Observers:      info.Observers,
		})
	}

	sort.Slice(resp.MemberInfos, func(i, j int) bool {
		return resp.MemberInfos[i].ShardId < resp.MemberInfos[j].ShardId
	})

	for target, shard := range shardInfos.Targets {
		resp.ShardInfos.Targets[target] = &pb.TargetShard{
			GrpcAddr: shard.GrpcAddr,
			ShardIds: shard.ShardIds,
		}
	}

	for shardId, targets := range shardInfos.Shards {
		resp.ShardInfos.Shards[shardId] = &pb.RaftInfoResponse_Target{
			Targets: targets,
		}
	}

	return resp, nil
}
