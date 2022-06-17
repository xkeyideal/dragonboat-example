package raft

import (
	"context"
	"errors"

	pb "github.com/xkeyideal/dragonboat-example/v3/moveto"
	"github.com/xkeyideal/dragonboat-example/v3/raft/command"

	"go.uber.org/zap"
)

type RaftOpType uint8

const (
	DELETE RaftOpType = iota + 1
	ADD
	OBSERVER
)

type MoveTo struct {
	pb.UnimplementedMoveToServer
	s *Storage
}

func NewMoveTo(s *Storage) *MoveTo {
	return &MoveTo{
		s: s,
	}
}

func (m *MoveTo) MoveToInvoke(ctx context.Context, req *pb.MoveToCommand) (*pb.MoveToResponse, error) {
	cmd, err := command.DecodeCmd(req.Cmd)
	if err != nil {
		return nil, err
	}

	revision, localAddr, linearAddr, err := m.s.moveToAddress(req.ClusterId, cmd.Linear())
	if err != nil {
		m.s.log.Warn("[raftstorage] [invoke] [MoveToInvoke]",
			zap.String("target", m.s.target),
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

	// 当本地的版本号>=MoveTo之前的版本号，才判断是否本节点是否可以命中
	if revision >= req.Revision && m.s.moveToCheck(req.ClusterId) {
		// 查询本地建议执行的地址
		return &pb.MoveToResponse{
			Status:        pb.MoveToStatus_Miss,
			Revision:      revision,
			RecommendAddr: addr,
		}, nil
	}

	// 直接尝试本地是否能够执行
	res, err := m.s.invoke(req.ClusterId, cmd)
	if err != nil {
		return nil, err
	}

	return &pb.MoveToResponse{
		Status: pb.MoveToStatus_Success,
		Result: res,
	}, nil
}

func (m *MoveTo) RaftNodeInvoke(ctx context.Context, req *pb.RaftInvokeOp) (*pb.MoveToResponse, error) {
	revision, localAddr, linearAddr, err := m.s.moveToAddress(req.ClusterId, req.Linear)
	if err != nil {
		m.s.log.Warn("[raftstorage] [invoke] [RaftNodeInvoke]",
			zap.String("target", m.s.target),
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
	if req.Linear && linearAddr != "" {
		addr = linearAddr
	}

	// 当本地的版本号>=MoveTo之前的版本号，才判断是否本节点是否可以命中
	if revision >= req.Revision && m.s.moveToCheck(req.ClusterId) {
		// 查询本地建议执行的地址
		return &pb.MoveToResponse{
			Status:        pb.MoveToStatus_Miss,
			Revision:      revision,
			RecommendAddr: addr,
		}, nil
	}

	var rerr error
	switch req.Op {
	case uint32(DELETE):
		rerr = m.s.removeRaftNode(req.NodeId, req.ClusterId)
	case uint32(ADD):
		rerr = m.s.addRaftNode(req.NodeId, req.Target, req.ClusterId)
	case uint32(OBSERVER):
		rerr = m.s.addRaftObserver(req.NodeId, req.Target, req.ClusterId)
	default:
		rerr = errors.New("RaftNodeInvoke OpType error")
	}

	if rerr != nil {
		return nil, rerr
	}

	return &pb.MoveToResponse{
		Status: pb.MoveToStatus_Success,
	}, nil
}
