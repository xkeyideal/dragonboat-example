package dragonboatexample

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	pb "github.com/xkeyideal/dragonboat-example/v3/pb/api"

	"github.com/xkeyideal/dragonboat-example/v3/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Engine struct {
	cfg *config.SystemConfig

	// sidecar conf storage
	raftStorage *raft.Storage

	grpcServer *grpc.Server

	kvServer          *KVServer
	raftControlServer *RaftControlServer

	// for webapi release rubik center start config
	raftConfCh chan *pb.RaftReleaseConfig
}

func NewEngine() *Engine {
	cfg := config.NewSystemConfig()

	engine := &Engine{
		raftConfCh: make(chan *pb.RaftReleaseConfig, 2),
		cfg:        cfg,
	}

	localAddr := fmt.Sprintf("%s:%d", cfg.IP, cfg.GrpcPort)
	engine.serverInit(localAddr, config.RaftDir)
	engine.grpcServer = engine.grpcInit(localAddr)

	listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", cfg.GrpcPort))
	if err != nil {
		log.Fatalf("[ERROR] grpc net listen %s\n", err.Error())
	}
	go func() {
		if err := engine.grpcServer.Serve(listener); err != nil {
			log.Fatalf("[ERROR] grpc start %s\n", err.Error())
		}
	}()

	log.Println("[INFO] grpc server started")

	// 为防止多个raft节点在同一台机器上启动，metadata的存储文件名采用grpcPort做区分
	raftConfig, clusterMessage := engine.loadRaftConfig(config.RaftDir, cfg.GrpcPort)
	rubikGrpcAddr := fmt.Sprintf("%s:%d", engine.cfg.IP, cfg.GrpcPort)

	b, _ := json.MarshalIndent(raftConfig, "", "    ")
	log.Printf("[INFO] generate raft config:\n%s\n", string(b))
	raftStorage, err := raft.NewStorage(rubikGrpcAddr, cfg.MetricsAddr, raftConfig)
	if err != nil {
		log.Fatalf("[ERROR] raft storage %s\n", err.Error())
	}

	err = raftStorage.RaftReady()
	if err != nil {
		log.Fatalf("[ERROR] raft ready %s\n", err.Error())
	}

	// raft集群成功启动后，无需等待gossip同步集群分组信息，直接将文件里的数据加载到内存里
	// 如果revision发生了变化，此处也能快速收敛
	raftStorage.UpdateShardMessage(clusterMessage)
	engine.raftStorage = raftStorage

	// raft启动成功后，设置对外提供的grpc服务ready
	engine.kvServer.ChangeServing(config.Serving)
	engine.raftControlServer.ChangeServing(config.Serving)

	log.Printf("[INFO] running raft gossip model: %v, localAddr: %s\n", raftConfig.Gossip, localAddr)

	return engine
}

func (engine *Engine) serverInit(localAddr, raftDir string) {
	engine.kvServer = NewKVServer(config.ColumnFamilyDefault, engine.raftStorage)
	engine.raftControlServer = NewRaftControlServer(
		config.ColumnFamilyDefault, engine.cfg.IP, raftDir, engine.cfg.GrpcPort,
		engine.raftStorage, engine.raftConfCh,
	)
}

func (engine *Engine) grpcInit(localAddr string) *grpc.Server {
	kaep := keepalive.EnforcementPolicy{
		MinTime:             time.Duration(engine.cfg.KeepAliveMinTime) * time.Second,
		PermitWithoutStream: true,
	}

	kasp := keepalive.ServerParameters{
		Time:    2 * time.Minute,
		Timeout: 5 * time.Second,
	}

	gopts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
		grpc.WriteBufferSize(engine.cfg.WriteBufferSize),
		grpc.ReadBufferSize(engine.cfg.ReadBufferSize),
		grpc.MaxRecvMsgSize(engine.cfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(engine.cfg.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(engine.cfg.MaxConcurrentStreams),
	}

	server := grpc.NewServer(gopts...)

	pb.RegisterKVServer(server, engine.kvServer)
	pb.RegisterRaftControlServer(server, engine.raftControlServer)

	return server
}

func (engine *Engine) Close() {
	if engine.grpcServer != nil {
		engine.grpcServer.Stop()
	}

	log.Println("[INFO] engine closed")
}
