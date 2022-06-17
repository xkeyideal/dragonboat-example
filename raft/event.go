package raft

import (
	"sync/atomic"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"

	"github.com/lni/dragonboat/v3/raftio"
	"go.uber.org/zap"
)

type raftEvent struct {
	s *Storage
}

func (e *raftEvent) LeaderUpdated(info raftio.LeaderInfo) {
	e.s.log.Warn("raft storage event LeaderUpdated",
		zap.String("target", e.s.target),
		zap.Any("info", info),
	)

	if atomic.LoadUint32(&e.s.status) == ready && info.LeaderID != 0 {
		e.s.leaderc <- info
	}
}

type systemEvent struct {
	s *Storage
}

func (e *systemEvent) NodeHostShuttingDown() {
	e.s.log.Warn("raft storage event NodeHostShuttingDown", zap.String("target", e.s.target))
}

func (e *systemEvent) NodeUnloaded(info raftio.NodeInfo) {
	e.s.log.Warn("raft storage event NodeUnloaded", zap.String("target", e.s.target), zap.Any("info", info))
}

func (e *systemEvent) NodeReady(info raftio.NodeInfo) {
	e.s.log.Info("raft storage event NodeReady", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) MembershipChanged(info raftio.NodeInfo) {
	e.s.log.Warn("raft storage event MembershipChanged", zap.String("target", e.s.target), zap.Any("info", info))
	if atomic.LoadUint32(&e.s.status) == ready {
		e.s.memberc <- info
	}
}
func (e *systemEvent) ConnectionEstablished(info raftio.ConnectionInfo) {
	e.s.log.Info("raft storage event ConnectionEstablished", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) ConnectionFailed(info raftio.ConnectionInfo) {
	e.s.log.Warn("raft storage event ConnectionFailed", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SendSnapshotStarted(info raftio.SnapshotInfo) {
	e.s.log.Info("raft storage event SendSnapshotStarted", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	e.s.log.Info("raft storage event SendSnapshotCompleted", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SendSnapshotAborted(info raftio.SnapshotInfo) {
	e.s.log.Info("raft storage event SendSnapshotAborted", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SnapshotReceived(info raftio.SnapshotInfo) {
	e.s.log.Info("raft storage event SnapshotReceived", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SnapshotRecovered(info raftio.SnapshotInfo) {
	e.s.log.Info("raft storage event SnapshotRecovered", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SnapshotCreated(info raftio.SnapshotInfo) {
	e.s.log.Warn("raft storage event SnapshotCreated", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) SnapshotCompacted(info raftio.SnapshotInfo) {
	e.s.log.Warn("raft storage event SnapshotCompacted", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) LogCompacted(info raftio.EntryInfo) {
	e.s.log.Info("raft storage event LogCompacted", zap.String("target", e.s.target), zap.Any("info", info))
}
func (e *systemEvent) LogDBCompacted(info raftio.EntryInfo) {
	e.s.log.Info("raft storage event LogDBCompacted", zap.String("target", e.s.target), zap.Any("info", info))
}

func (s *Storage) handleEvents() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case info := <-s.memberc:
			if info.NodeID == s.cfg.ReplicaId {
				m, err := s.getShardMembership(info.ClusterID)
				if err != nil {
					continue
				}

				s.cmu.Lock()
				s.memberCache[info.ClusterID] = m
				s.cmu.Unlock()
			}
		case info := <-s.leaderc:
			if info.NodeID == s.cfg.ReplicaId {
				m, err := s.getShardMembership(info.ClusterID)
				if err != nil {
					continue
				}

				s.cmu.Lock()
				s.memberCache[info.ClusterID] = m
				s.cmu.Unlock()
			}
		case <-ticker.C:
			s.cmu.Lock()
			if len(s.memberCache) > 0 {
				mc := s.memberCache
				s.gossip.UpdateMembershipMessage(&gossip.RaftMembershipMessage{
					MemberInfos: mc,
				})

				s.memberCache = make(map[uint64]*gossip.MemberInfo)
			}
			s.cmu.Unlock()
		case <-s.stopper.ShouldStop():
			return
		}
	}
}
