package raft

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	"github.com/xkeyideal/dragonboat-example/v3/ilogger"
	"github.com/xkeyideal/dragonboat-example/v3/store"

	zlog "github.com/xkeyideal/dragonboat-example/v3/internal/logger"
	pb "github.com/xkeyideal/dragonboat-example/v3/moveto"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/syncutil"
	"github.com/xkeyideal/gokit/httpkit"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func initLogger(replicaId uint64, target string) {
	ilogger.Lo.SetReplicaId(replicaId)
	ilogger.Lo.SetTarget(target)
}

var (
	unready uint32 = 0
	ready   uint32 = 1
)

type Storage struct {
	status  uint32
	cfg     *RaftConfig
	dataDir string

	nh *dragonboat.NodeHost

	mu    sync.RWMutex
	smMap map[uint64]*store.Store
	csMap map[uint64]*client.Session

	// 当前节点的地址，若是地址不变则取raftAddr,若采用gossip方式起则是nhid-xxxxx
	target string

	log *zap.Logger

	stopper *syncutil.Stopper

	// 用来记录shard leader的变化
	leaderc chan raftio.LeaderInfo

	// 用来记录shard membership的变化
	memberc chan raftio.NodeInfo

	cmu         sync.Mutex
	memberCache map[uint64]*gossip.MemberInfo

	// 用于同步集群信息与shard分组信息
	gossip *gossip.GossipManager

	// grpc服务，用于接收moveTo的command
	grpcServer *grpc.Server

	wg    sync.WaitGroup
	exitc chan struct{}
}

func NewStorage(ServerGrpcAddr, metricsAddr string, cfg *RaftConfig) (*Storage, error) {
	// 存储目录
	if err := mkdir(cfg.StorageDir); err != nil {
		return nil, err
	}

	// 初始化raft内部的日志
	if err := mkdir(cfg.LogDir); err != nil {
		return nil, err
	}

	// 初始化raft和数据的存储路径
	raftDir, dataDir, err := initPath(cfg.StorageDir, cfg.ReplicaId)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		cfg:         cfg,
		status:      unready,
		dataDir:     dataDir,
		csMap:       make(map[uint64]*client.Session, len(cfg.ShardIds)),
		smMap:       make(map[uint64]*store.Store, len(cfg.ShardIds)),
		stopper:     syncutil.NewStopper(),
		log:         zlog.NewLogger(filepath.Join(cfg.LogDir, "raft-storage.log"), cfg.LogLevel, false),
		leaderc:     make(chan raftio.LeaderInfo, 24),
		memberc:     make(chan raftio.NodeInfo, 24),
		memberCache: make(map[uint64]*gossip.MemberInfo),
		exitc:       make(chan struct{}),
	}

	set := make(map[uint64]struct{})
	for _, jn := range cfg.Join {
		for replicaId := range jn {
			set[replicaId] = struct{}{}
		}
	}

	// 初始化服务自使用的gossip同步服务
	gossipName := fmt.Sprintf("gossip-%d", cfg.ReplicaId)
	cfg.GossipConfig.SetShardCallback(s.WriteShardToFile)
	gossipOpts := gossip.GossipOptions{
		Name:               gossipName,
		MoveToGrpcAddr:     fmt.Sprintf("%s:%d", cfg.HostIP, cfg.GrpcPort),
		ServerGrpcAddr:     ServerGrpcAddr,
		LogDir:             cfg.LogDir,
		LogLevel:           cfg.LogLevel,
		GossipNodes:        len(set) / 2,
		DisableCoordinates: false,
	}
	g, err := gossip.NewGossipManager(cfg.GossipConfig, gossipOpts)
	if err != nil {
		return nil, err
	}
	s.gossip = g

	// dragonboat raft的事件处理
	raftEvent := &raftEvent{s}
	systemEvent := &systemEvent{s}
	var nhc config.NodeHostConfig
	// 根据raft寻址配置，确定采用gossip方式寻址或固定Addr方式寻址
	// 此处的gossip是dragonboat内部的，与上面的gossip不同
	if cfg.Gossip {
		s.target = fmt.Sprintf("nhid-%d", cfg.ReplicaId)
		nhc = buildNodeHostConfigByGossip(
			raftDir, cfg.ReplicaId, cfg.GossipPort,
			cfg.HostIP, cfg.RaftAddr, cfg.GossipSeeds,
			cfg.Metrics, raftEvent, systemEvent,
		)
	} else {
		s.target = cfg.RaftAddr
		nhc = buildNodeHostConfig(raftDir, cfg.RaftAddr, cfg.Metrics, raftEvent, systemEvent)
	}

	initLogger(cfg.ReplicaId, s.target)

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	s.nh = nh

	// 启动dragonboat的event变化的回调处理
	s.stopper.RunWorker(s.handleEvents)

	// 根据分配好的每个节点归属的shardIds来初始化实例
	err = s.stateMachine(cfg.Join, dataDir, cfg.ShardIds)
	if err != nil {
		return nil, err
	}

	// grpc服务启动
	listener, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", cfg.HostIP, cfg.GrpcPort))
	if err != nil {
		return nil, err
	}
	s.grpcServer = s.grpcInit()

	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Fatalf("[ERROR] raft %s moveTo grpc server start up failed, err: %s", s.target, err.Error())
		}
	}()

	log.Println("[INFO] raft", s.target, "moveTo grpc started", cfg.HostIP, cfg.GrpcPort, cfg.ShardIds)

	// 初始化gossip meta
	err = s.gossip.SetNodeMeta(gossip.Meta{
		MoveToGrpcAddr: gossipOpts.MoveToGrpcAddr,
		ServerGrpcAddr: gossipOpts.ServerGrpcAddr,
	})
	if err != nil {
		return nil, err
	}

	// 启动metrics收集程序
	if cfg.Metrics {
		go s.metricsService(metricsAddr)
	}

	return s, nil
}

func (s *Storage) RaftReady() error {
	ch := make(chan struct{}, 1)
	go s.nodeReady(s.cfg.ShardIds, ch)

	select {
	case <-ch:
	}

	// 集群启动后，先同步一遍membership
	membership := make(map[uint64]*gossip.MemberInfo)
	for _, shardId := range s.cfg.ShardIds {
		info, err := s.getShardMembership(shardId)
		if err != nil {
			log.Println("[WARN] RaftReady get shard membership:", shardId, err.Error())
			continue
		}

		membership[shardId] = info
	}

	s.gossip.UpdateMembershipMessage(&gossip.RaftMembershipMessage{
		MemberInfos: membership,
	})

	atomic.StoreUint32(&s.status, ready)
	log.Println("[INFO] raft", s.target, "started", s.nh.ID())

	return nil
}

func (s *Storage) metricsService(metricsAddr string) {
	s.wg.Add(1)
	ticker := time.NewTicker(2 * time.Second)
	addr := fmt.Sprintf("%s/%s_%s", metricsAddr, s.target, s.cfg.RaftAddr)

	client := httpkit.NewHttpClient(2*time.Second, 1, 100*time.Millisecond, time.Second, nil)

	for {
		select {
		case <-ticker.C:
			s.metricsFlush(addr, client)
		case <-s.exitc:
			s.wg.Done()
			s.metricsFlush(addr, client)
			return
		}
	}
}

func (s *Storage) metricsFlush(addr string, client *httpkit.HttpClient) {
	buf := bytes.NewBuffer(nil)
	dragonboat.WriteHealthMetrics(buf)

	client = client.SetBody(buf).SetHeader("Content-Type", "application/octet-stream")
	_, err := client.Post(addr)
	if err != nil {
		s.log.Warn("[raftstorage] [metrics] [upload]", zap.Error(err))
	}
}

func (s *Storage) stateMachine(join map[uint64]map[uint64]bool, dataDir string, shardIds []uint64) error {
	var (
		initialMembers map[uint64]string
		jn             map[uint64]bool
		ok             bool
		nodeJoin       bool
	)

	// 根据分配好的每个节点归属的shardIds来初始化实例
	for _, shardId := range shardIds {
		jn, ok = join[shardId]
		if !ok {
			return fmt.Errorf("raft shardId: %d, can't find join config", shardId)
		}

		nodeJoin, ok = jn[s.cfg.ReplicaId]
		if !ok {
			return fmt.Errorf("raft shardId: %d, replicaId: %d, can't find join config", shardId, s.cfg.ReplicaId)
		}

		rc := buildRaftConfig(s.cfg.ReplicaId, shardId)
		shardDataPath := filepath.Join(dataDir, strconv.Itoa(int(shardId)))
		opts := store.PebbleShardOption{
			Target:    s.target,
			ReplicaId: s.cfg.ReplicaId,
			ShardId:   shardId,
		}
		store, err := store.NewStore(shardId, shardDataPath, opts, s.log)
		if err != nil {
			return err
		}

		if !nodeJoin {
			initialMembers, ok = s.cfg.InitialMembers[shardId]
			if !ok {
				return fmt.Errorf("raft shardId: %d, can't find initial members", shardId)
			}
		} else {
			initialMembers = make(map[uint64]string)
		}

		stateMachine := newStateMachine(shardId, s.cfg.ReplicaId, store)
		err = s.nh.StartOnDiskCluster(initialMembers, nodeJoin, func(_ uint64, _ uint64) sm.IOnDiskStateMachine {
			return stateMachine
		}, rc)
		if err != nil {
			return err
		}

		s.mu.Lock()
		s.csMap[shardId] = s.nh.GetNoOPSession(shardId)
		s.smMap[shardId] = store
		s.mu.Unlock()
	}

	return nil
}

func (s *Storage) grpcInit() *grpc.Server {
	grpcCfg := buildGRPCConfig()

	kaep := keepalive.EnforcementPolicy{
		MinTime:             time.Duration(grpcCfg.KeepAliveMinTime) * time.Second,
		PermitWithoutStream: true,
	}

	kasp := keepalive.ServerParameters{
		Time:    2 * time.Hour,
		Timeout: 20 * time.Second,
	}

	gopts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.WriteBufferSize(grpcCfg.WriteBufferSize),
		grpc.ReadBufferSize(grpcCfg.ReadBufferSize),
		grpc.MaxRecvMsgSize(grpcCfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(grpcCfg.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(grpcCfg.MaxConcurrentStreams),
	}
	server := grpc.NewServer(gopts...)
	pb.RegisterMoveToServer(server, NewMoveTo(s))

	return server
}

// 读取raft启动的meta数据
func ReadMetadataFromFile(dir string, id uint16) (*RaftMetadata, error) {
	return readMetadataFromFile(filepath.Join(dir, fmt.Sprintf("%s_%d.json", metadataFileName, id)))
}

// 存储raft启动的meta数据
func WriteMetadataToFile(dir string, id uint16, meta *RaftMetadata) error {
	return writeMetadataToFile(filepath.Join(dir, fmt.Sprintf("%s_%d.json", metadataFileName, id)), meta)
}

// ReadShardFromFile 集群启动成功，外部管理Storage的程序需要调用读取本地文件的shard信息
func ReadShardFromFile(dir string, replicaId uint64) (*gossip.RaftShardMessage, error) {
	return readShardFromFile(filepath.Join(dir, fmt.Sprintf("%s_%d.json", shardFileName, replicaId)))
}

// 更新集群后需要存储到文件
func (s *Storage) WriteShardToFile(shard *gossip.RaftShardMessage) error {
	return writeShardToFile(filepath.Join(s.cfg.StorageDir, fmt.Sprintf("%s_%d.json", shardFileName, s.cfg.ReplicaId)), shard)
}

// 更新集群的情况
func (s *Storage) UpdateShardMessage(shard *gossip.RaftShardMessage) {
	s.gossip.UpdateShardMessage(shard)
}

// shardIds 统一信任gossip的管理
func (s *Storage) getShardIds() []uint64 {
	shard := s.gossip.GetShardMessage()

	return shard.Targets[s.target].ShardIds
}

func (s *Storage) getShardId(hashKey string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(hashKey)) % s.cfg.MultiGroupSize)
}

func (s *Storage) GetReplicaId() uint64 {
	return s.cfg.ReplicaId
}

func (s *Storage) GetTarget() string {
	return s.target
}

func (s *Storage) shardMembership(shardId uint64) (*dragonboat.Membership, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rafttimeout)
	defer cancel()
	return s.nh.SyncGetClusterMembership(ctx, shardId)
}

func (s *Storage) getShardMembership(shardId uint64) (*gossip.MemberInfo, error) {
	membership, err := s.shardMembership(shardId)
	if err != nil {
		return nil, err
	}

	leaderID, valid, err := s.nh.GetLeaderID(shardId)
	if err != nil {
		return nil, err
	}

	return &gossip.MemberInfo{
		ShardId:        shardId,
		ConfigChangeId: membership.ConfigChangeID,
		Nodes:          membership.Nodes,
		Observers:      membership.Observers,
		LeaderId:       leaderID,
		LeaderValid:    valid,
	}, nil
}

func (s *Storage) GetRaftMembership() ([]*gossip.MemberInfo, error) {
	membership := []*gossip.MemberInfo{}
	for _, shardId := range s.getShardIds() {
		info, err := s.getShardMembership(shardId)
		if err != nil {
			return nil, err
		}

		membership = append(membership, info)
	}

	return membership, nil
}

func (s *Storage) GetLocalMembership() map[uint64]*gossip.MemberInfo {
	return s.gossip.GetMembershipMessages()
}

func (s *Storage) GetNodeHost() []string {
	inst := s.gossip.GetserverInstances()

	addrs := []string{}

	for addr := range inst {
		addrs = append(addrs, addr)
	}

	return addrs
}

func (s *Storage) GetAliveInstances() map[string]bool {
	return s.gossip.GetAliveInstances()
}

func (s *Storage) GetShardMessage() *gossip.RaftShardMessage {
	return s.gossip.GetShardMessage()
}

func (s *Storage) StopRaftNode() {
	atomic.StoreUint32(&s.status, unready)

	if s.nh != nil {
		s.nh.Stop()
		s.nh = nil
	}

	if s.cfg.Metrics {
		close(s.exitc)
		s.wg.Wait()
	}

	s.log.Sync()
	s.stopper.Close()
	s.gossip.Close()
	s.grpcServer.Stop()
}

func (s *Storage) nodeReady(shardIds []uint64, ch chan<- struct{}) {
	wg := sync.WaitGroup{}
	wg.Add(len(shardIds))
	for _, shardId := range shardIds {
		go func(shardId uint64) {
			shardReady := false
			for {
				_, ready, err := s.nh.GetLeaderID(shardId)
				if err == nil && ready {
					shardReady = true
					break
				}

				if err != nil {
					log.Println("nodeReady", s.target, shardId, err.Error())
				}

				time.Sleep(1000 * time.Millisecond)
			}

			if shardReady {
				log.Println("nodeReady", s.target, shardId, "ready")
			}

			wg.Done()
		}(shardId)
	}

	wg.Wait()

	ch <- struct{}{}
}

func initPath(path string, replicaId uint64) (string, string, error) {
	raftPath := filepath.Join(path, fmt.Sprintf("raft_node%d", replicaId))
	dataPath := filepath.Join(path, fmt.Sprintf("data_node%d", replicaId))

	if err := mkdir(raftPath); err != nil {
		return "", "", err
	}

	if err := mkdir(dataPath); err != nil {
		return "", "", err
	}

	return raftPath, dataPath, nil
}

func mkdir(dir string) error {
	if !pathIsExist(dir) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}
