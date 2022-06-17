package dragonboatexample

import (
	"context"
	"sync/atomic"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	"github.com/xkeyideal/dragonboat-example/v3/kv"
	pb "github.com/xkeyideal/dragonboat-example/v3/pb/api"
	"github.com/xkeyideal/dragonboat-example/v3/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVServer struct {
	// pb.UnimplementedKVServer

	serving int32
	kv      *kv.KVStorage
}

func NewKVServer(cf string, raftStorage *raft.Storage) *KVServer {
	return &KVServer{
		serving: config.Abort,
		kv:      kv.NewKVStorage(cf, raftStorage),
	}
}

func (kv *KVServer) ChangeServing(serve int32) {
	atomic.StoreInt32(&kv.serving, serve)
}

func (kv *KVServer) ready() bool {
	return atomic.LoadInt32(&kv.serving) == config.Serving
}

func (kv *KVServer) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	if !kv.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	revision, val, err := kv.kv.Get(in.Key, in.Linearizable)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return &pb.GetResponse{
		Key:      in.Key,
		Val:      val,
		Revision: revision,
	}, nil
}

func (kv *KVServer) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	if !kv.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	revision, err := kv.kv.Put(in.Key, in.Val)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return &pb.PutResponse{
		Revision: revision,
	}, nil
}

func (kv *KVServer) Del(ctx context.Context, in *pb.DelRequest) (*pb.DelResponse, error) {
	if !kv.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	revision, val, err := kv.kv.Get(in.Key, false)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	err = kv.kv.Del(in.Key)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	return &pb.DelResponse{
		Key:      in.Key,
		Val:      val,
		Revision: revision,
	}, nil
}

type RaftControlServer struct {
	//pb.UnimplementedRaftControlServer

	serving    int32
	raftStatus int32
	raftConfCh chan *pb.RaftReleaseConfig

	localIp  string
	raftDir  string
	grpcPort uint16

	meta *kv.RaftMeta
}

func NewRaftControlServer(cf, localIp, raftDir string, grpcPort uint16,
	raftStorage *raft.Storage, raftConfCh chan *pb.RaftReleaseConfig) *RaftControlServer {
	return &RaftControlServer{
		serving:    config.Abort,
		raftStatus: 1,
		localIp:    localIp,
		raftConfCh: raftConfCh,
		raftDir:    raftDir,
		grpcPort:   grpcPort,
		meta:       kv.NewRaftMeta(cf, raftStorage),
	}
}

func (rc *RaftControlServer) ChangeServing(serve int32) {
	atomic.StoreInt32(&rc.serving, serve)
}

func (rc *RaftControlServer) ready() bool {
	return atomic.LoadInt32(&rc.serving) == config.Serving
}

func (rc *RaftControlServer) StopRaftStatus() {
	atomic.StoreInt32(&rc.raftStatus, 0)
}

// ReleaseRaftConf 前端推送启动配置接口，不需要判断ready
func (rc *RaftControlServer) ReleaseRaftConf(ctx context.Context, in *pb.RaftReleaseConfig) (*pb.WEmpty, error) {
	if atomic.LoadInt32(&rc.raftStatus) == 1 {
		rc.raftConfCh <- in
		atomic.StoreInt32(&rc.raftStatus, 0)
	} else {
		// 启动后，前端接口再次推送，系统更新metadata
		metadata, err := raft.ReadMetadataFromFile(rc.raftDir, rc.grpcPort)
		if err != nil {
			return &pb.WEmpty{}, nil
		}

		if metadata.Revision >= in.Revision {
			return &pb.WEmpty{}, nil
		}

		// 如果本地没有metadata则存储入文件, 可以忽略错误
		raft.WriteMetadataToFile(rc.raftDir, rc.grpcPort, MetadataByRelease(rc.localIp, in))
	}

	return &pb.WEmpty{}, nil
}

func (rc *RaftControlServer) ReleaseRaftShard(ctx context.Context, in *pb.RaftShard) (*pb.WEmpty, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	shardMap := make(map[uint64][]string)
	targetMap := make(map[string]gossip.TargetShardId)

	for target, tc := range in.TargetMap {
		targetMap[target] = gossip.TargetShardId{
			GrpcAddr: tc.GrpcAddr,
			ShardIds: tc.ShardIds,
		}

		for _, shardId := range tc.ShardIds {
			shardMap[shardId] = append(shardMap[shardId], target)
		}
	}

	initialMembers := make(map[uint64]map[uint64]string)
	for shardId, im := range in.InitialMembers {
		initialMembers[shardId] = im.InitialMembers
	}

	join := make(map[uint64]map[uint64]bool)
	for shardId, jn := range in.Join {
		join[shardId] = jn.Join
	}

	clusterMessage := &gossip.RaftShardMessage{
		Revision:       in.Revision,
		Targets:        targetMap,
		Shards:         shardMap,
		InitialMembers: initialMembers,
		Join:           join,
	}

	rc.meta.UpdateShardMessage(clusterMessage)

	return &pb.WEmpty{}, nil
}

func (rc *RaftControlServer) UpdateRaftShardIds(ctx context.Context, in *pb.RaftShardIds) (*pb.WEmpty, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	err := rc.meta.UpdateRaftShardIds(in.Add, in.ShardIds)
	return &pb.WEmpty{}, err
}

func (rc *RaftControlServer) RaftShardInfo(ctx context.Context, in *pb.WEmpty) (*pb.RaftInfoResponse, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	return rc.meta.RaftShardInfo()
}

func (rc *RaftControlServer) AddRaftObserverReplica(ctx context.Context, in *pb.RaftOp) (*pb.WEmpty, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	err := rc.meta.AddRaftObserverNode(in.ReplicaId, in.Target, in.ShardIds)

	return &pb.WEmpty{}, err
}

func (rc *RaftControlServer) AddRaftReplica(ctx context.Context, in *pb.RaftOp) (*pb.WEmpty, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	err := rc.meta.AddRaftNode(in.ReplicaId, in.Target, in.ShardIds)

	return &pb.WEmpty{}, err
}

func (rc *RaftControlServer) RemoveRaftReplica(ctx context.Context, in *pb.RaftOp) (*pb.WEmpty, error) {
	if !rc.ready() {
		return nil, status.Error(codes.Aborted, config.ErrSystemAborted.Error())
	}

	err := rc.meta.RemoveRaftNode(in.ReplicaId, in.ShardIds)

	return &pb.WEmpty{}, err
}
