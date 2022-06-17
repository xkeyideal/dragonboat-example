package raft

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	pb "github.com/xkeyideal/dragonboat-example/v3/pb/moveto"
	"github.com/xkeyideal/dragonboat-example/v3/raft/command"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func (s *Storage) moveToCheck(shardId uint64) bool {
	shardIds := s.getShardIds()
	for i := 0; i < len(shardIds); i++ {
		if shardId == shardIds[i] {
			return false
		}
	}

	return true
}

func localAddress(shardId uint64, alives map[string]bool, rcm *gossip.RaftShardMessage) (string, error) {
	nhids, ok := rcm.Shards[shardId]
	if !ok {
		return "", fmt.Errorf("local shardId: %d not found raft machine", shardId)
	}

	address := ""
	alive := false
	size := len(nhids)
	for i, k := 0, rand.Intn(size); i < size; i++ {
		nhid := nhids[k%size]
		if target, ok := rcm.Targets[nhid]; ok {
			alive = alives[target.GrpcAddr]
			if alive {
				address = target.GrpcAddr
				break
			}
		}

		k++
	}

	if !alive {
		return "", fmt.Errorf("local shardId: %d all machine offline", shardId)
	}

	return address, nil
}

// 返回非线性、线性操作的推荐MoveTo地址
func (s *Storage) moveToAddress(shardId uint64, linear bool) (int64, string, string, error) {
	rcm := s.gossip.GetShardMessage()

	var (
		target  gossip.TargetShardId
		nhid    string
		address string
		ok      bool
		err     error
	)

	// 获取alive的MoveTo地址
	alives := s.gossip.GetAliveInstances()

	// 先获取非线性的可用地址
	address, err = localAddress(shardId, alives, rcm)
	if err != nil {
		return 0, "", "", err
	}

	if !linear {
		return rcm.Revision, address, "", nil
	}

	// 线性操作，从membership里确定leader，若leader已经offline，则在集群中随机找一个online的
	rmm := s.gossip.GetMembershipMessage(shardId)
	if rmm == nil {
		return rcm.Revision, address, "", fmt.Errorf("linear shardId: %d not found membership", shardId)
	}

	nhid, ok = rmm.Replicas[rmm.LeaderId]
	if !ok {
		return rcm.Revision, address, "", fmt.Errorf("linear shardId: %d not found leader node", shardId)
	}

	target, ok = rcm.Targets[nhid]
	if !ok {
		return rcm.Revision, address, "", fmt.Errorf("linear shardId: %d not found leader target", shardId)
	}

	alive := alives[target.GrpcAddr]

	// 如果leader节点已经death
	if !alive {
		return rcm.Revision, address, "", fmt.Errorf("linear shardId: %d leader death", shardId)
	}

	return rcm.Revision, address, target.GrpcAddr, nil
}

// moveTo的时候可以直接传shardId代替传hashkey，因为shardId的算法都是一样的，
// 不可能出现不同的节点计算出的shardId不一致的情况
func (s *Storage) moveToInvoke(shardId uint64, cmd command.RaftCommand) ([]byte, error) {
	gcall := func(ctx context.Context, shardId uint64, revision int64,
		addr string, b []byte) (*pb.MoveToResponse, error) {

		// 1. grpc call addr
		conn, err := grpc.DialContext(ctx, addr, []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
		}...)
		if err != nil {
			return nil, err
		}
		client := pb.NewMoveToClient(conn)

		resp, err := client.MoveToInvoke(ctx, &pb.MoveToCommand{
			ShardId:  shardId,
			Revision: revision,
			Cmd:      b,
		}, []grpc.CallOption{
			grpc.WaitForReady(false),
		}...)

		return resp, err
	}

	b, err := command.EncodeCmd(cmd)
	if err != nil {
		return nil, err
	}

	retry := false
	revision, localAddr, linearAddr, err := s.moveToAddress(shardId, cmd.Linear())
	if err != nil {
		s.log.Warn("raft storage invoke MoveToInvoke",
			zap.String("target", s.target),
			zap.String("localAddr", localAddr),
			zap.String("linearAddr", linearAddr),
			zap.Error(err),
		)
	}
	// 非线性、线性地址均查不到，且报错，才记录操作
	if localAddr == "" && linearAddr == "" && err != nil {
		return nil, err
	}

	addr := localAddr
	if cmd.Linear() && linearAddr != "" {
		addr = linearAddr
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), moveToTimeout)
		resp, err := gcall(ctx, shardId, revision, addr, b)
		cancel()
		if err != nil {
			return nil, err
		}

		if resp.Status == pb.MoveToStatus_Success {
			return resp.Result, nil
		}

		if retry {
			break
		}

		// 只有当前shard集群分配的版本号 <= MoveTo返回的版本号，才使用MoveTo推荐的地址去重试
		// 并且只保证重试一次
		if revision <= resp.Revision {
			retry = true
			addr = resp.RecommendAddr
			continue
		}

		break
	}

	return nil, moveToErr
}

func (s *Storage) raftNodeInvoke(replicaId, shardId uint64, target string, op uint32, linear bool) ([]byte, error) {
	gcall := func(ctx context.Context, replicaId, shardId uint64, target string, op uint32,
		revision int64, addr string, linear bool) (*pb.MoveToResponse, error) {
		// 1. grpc call addr
		conn, err := grpc.DialContext(ctx, addr, []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
		}...)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := pb.NewMoveToClient(conn)

		resp, err := client.RaftNodeInvoke(ctx, &pb.RaftInvokeOp{
			ReplicaId: replicaId,
			ShardId:   shardId,
			Target:    target,
			Op:        op,
			Linear:    linear,
			Revision:  revision,
		}, []grpc.CallOption{
			grpc.WaitForReady(false),
		}...)

		return resp, err
	}

	retry := false

	revision, localAddr, linearAddr, err := s.moveToAddress(shardId, linear)
	if err != nil {
		s.log.Warn("raft storage invoke RaftNodeInvoke",
			zap.String("target", s.target),
			zap.String("localAddr", localAddr),
			zap.String("linearAddr", linearAddr),
			zap.Error(err),
		)
	}
	// 非线性、线性地址均查不到，且报错，才记录操作
	if localAddr == "" && linearAddr == "" && err != nil {
		return nil, err
	}

	addr := localAddr
	if linear && linearAddr != "" {
		addr = linearAddr
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), moveToTimeout)
		resp, err := gcall(
			ctx,
			replicaId, shardId, target, op,
			revision, addr, linear,
		)
		cancel()
		if err != nil {
			return nil, err
		}

		if resp.Status == pb.MoveToStatus_Success {
			return resp.Result, nil
		}

		if retry {
			break
		}

		// 只有当前shard集群分配的版本号 <= MoveTo返回的版本号，才使用MoveTo推荐的地址去重试
		// 并且只保证重试一次
		if revision <= resp.Revision {
			retry = true
			addr = resp.RecommendAddr
			continue
		}

		break
	}

	return nil, moveToErr
}

func (s *Storage) invoke(shardId uint64, cmd command.RaftCommand) ([]byte, error) {
	var err error
	if cmd.Linear() {
		s.mu.RLock()
		session, ok := s.csMap[shardId]
		s.mu.RUnlock()
		if !ok {
			return nil, sessionNotFound
		}
		ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
		err = cmd.RaftInvoke(ctx, s.nh, shardId, session)
		cancel()
	} else {
		s.mu.RLock()
		store, ok := s.smMap[shardId]
		s.mu.RUnlock()

		if !ok {
			return nil, storeNotFound
		}
		err = cmd.LocalInvoke(store)
	}

	if err != nil {
		return nil, err
	}

	return cmd.GetResp(), nil
}
