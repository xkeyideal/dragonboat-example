package dragonboatexample

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	pb "github.com/xkeyideal/dragonboat-example/v3/pb/api"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	"github.com/xkeyideal/dragonboat-example/v3/raft"
)

func (engine *Engine) loadRaftConfig(raftDir string, grpcPort uint16) (*raft.RaftConfig, *gossip.RaftShardMessage) {
	var (
		metadata        *raft.RaftMetadata
		shard           *gossip.RaftShardMessage
		releaseRaftConf *pb.RaftReleaseConfig
		shardOk         bool = false
		err             error
	)

	// 从文件中读取raft的启动配置
	metaOk := true
	metadata, err = raft.ReadMetadataFromFile(raftDir, grpcPort)
	if err != nil {
		if err == os.ErrNotExist {
			metaOk = false
		} else {
			log.Fatalf("[ERROR] %s\n", err.Error())
		}
	}

	if metaOk {
		shardOk = true
		shard, err = raft.ReadShardFromFile(raftDir, metadata.ReplicaId)
		if err != nil {
			if err == os.ErrNotExist {
				shardOk = false
			} else {
				log.Fatalf("[ERROR] %s\n", err.Error())
			}
		}
	}

	// 文件中metadata数据和集群分组数据都存在，那么直接启动
	if metaOk && shardOk {
		waitRelease := false
		if metadata.LocalIP != engine.cfg.IP {
			if !metadata.Gossip {
				log.Fatalf("[ERROR] metadata raft ip: %s not equal local ip: %s\n", metadata.LocalIP, engine.cfg.IP)
			} else {
				// IP 发生变化，则也需要等待webcenter下发数据
				waitRelease = true
				log.Printf("[WARN] raft by gossip metadata raft ip: %s not equal local ip: %s\n", metadata.LocalIP, engine.cfg.IP)
			}
		}

		// 集群分组的版本号大于metadata的版本号
		// 此种情况出现在: 集群里的某台机器修改了启动配置(常见于机器动态IP出现变化)，然后仅推送了该机器的启动配置
		// 通过raft协议更新了集群里其他机器的shard配置，导致shard的版本号比metadata的版本号要大
		if shard.Revision > metadata.Revision {
			waitRelease = true
			log.Printf("[WARN] raft shard revision %d greater metadata revision %d\n", shard.Revision, metadata.Revision)
		}

		if !waitRelease {
			// metadata数据和集群分组都有，则告知webcenter无需接收下发数据
			engine.raftControlServer.StopRaftStatus()
			key := fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.RaftPort)
			if metadata.Gossip {
				key = fmt.Sprintf("nhid-%d", metadata.ReplicaId)
			}

			// join, initialMembers的数据从shard获取
			return engine.raftConfigByMetadata(raftDir, shard.Targets[key].ShardIds, metadata, shard), shard
		}
	}

	log.Printf("[INFO] host: %s waiting send raft config...\n", engine.cfg.IP)

	// 等待发送启动配置
	select {
	case releaseRaftConf = <-engine.raftConfCh:
	}

	log.Printf("[INFO] host: %s received send raft config...\n", engine.cfg.IP)

	b, _ := json.MarshalIndent(releaseRaftConf, "", "    ")
	log.Printf("[INFO] received raft config:\n%s\n", string(b))

	// 集群分组信息是否待更新
	if !shardOk {
		shard = shardByRelease(releaseRaftConf)
	} else {
		// 本地的版本号大于控制中心下发的版本号
		if shard.Revision > releaseRaftConf.Revision {
			log.Fatalf("[ERROR] local shard revision: %d, greater release revision: %d\n", shard.Revision, releaseRaftConf.Revision)
		}

		if shard.Revision < releaseRaftConf.Revision {
			shard = shardByRelease(releaseRaftConf)
		}
	}

	// metadata 不存在或者版本号小于集群的版本号，需要根据推送的配置更新
	if !metaOk || shard.Revision > metadata.Revision {
		metadata = MetadataByRelease(engine.cfg.IP, releaseRaftConf)

		// 如果本地没有metadata则存储入文件, 可以忽略错误
		raft.WriteMetadataToFile(raftDir, grpcPort, metadata)
	}

	// 当采用raft启动时需要ip不能改变，使用engine.cfg.IP或metadata.LocalIP生成无区别
	// 当采用gossip启动时需要nodeId不能改变，也无区别
	key := fmt.Sprintf("%s:%d", engine.cfg.IP, releaseRaftConf.RaftPort)
	if releaseRaftConf.Gossip {
		key = fmt.Sprintf("nhid-%d", releaseRaftConf.ReplicaId)
	}

	// metadata是否待更新
	//if metaOk {
	// 判断MoveTo的地址是否正确
	for vkey, target := range shard.Targets {
		if vkey == key {
			if target.GrpcAddr != fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.GrpcPort) {
				log.Fatalf("[ERROR] current target: %s cluster moveTo grpcAddr changed %v->%v, please update grpcAddr and revision",
					key,
					target.GrpcAddr,
					fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.GrpcPort),
				)
			}
			break
		}
	}

	// 对存在的metadata与下发的进行校验
	err = validate(engine.cfg.IP, metadata, releaseRaftConf)
	if err != nil {
		log.Fatalf("[ERROR] %s\n", err.Error())
	}

	return engine.raftConfigByMetadata(raftDir, shard.Targets[key].ShardIds, metadata, shard), shard
}

func shardByRelease(releaseRaftConf *pb.RaftReleaseConfig) *gossip.RaftShardMessage {
	shardMap := make(map[uint64][]string)
	targetMap := make(map[string]gossip.TargetShardId)

	join := make(map[uint64]map[uint64]bool)
	for shardId, cj := range releaseRaftConf.Join {
		join[shardId] = cj.Join
	}

	initialMembers := make(map[uint64]map[uint64]string)
	for shardId, im := range releaseRaftConf.InitialMembers {
		initialMembers[shardId] = im.InitialMembers
	}

	for target, tc := range releaseRaftConf.TargetMap {
		targetMap[target] = gossip.TargetShardId{
			GrpcAddr: tc.GrpcAddr,
			ShardIds: tc.ShardIds,
		}

		for _, shardId := range tc.ShardIds {
			shardMap[shardId] = append(shardMap[shardId], target)
		}
	}

	return &gossip.RaftShardMessage{
		Revision:       releaseRaftConf.Revision,
		Targets:        targetMap,
		Shards:         shardMap,
		InitialMembers: initialMembers,
		Join:           join,
	}
}

func (engine *Engine) raftConfigByRelease(raftDir string, shardIds []uint64, releaseRaftConf *pb.RaftReleaseConfig) *raft.RaftConfig {
	join := make(map[uint64]map[uint64]bool)
	for shardId, cj := range releaseRaftConf.Join {
		join[shardId] = cj.Join
	}

	initialMembers := make(map[uint64]map[uint64]string)
	for shardId, im := range releaseRaftConf.InitialMembers {
		initialMembers[shardId] = im.InitialMembers
	}

	return &raft.RaftConfig{
		LogDir:         engine.cfg.LogDir,
		LogLevel:       engine.cfg.LogLevel,
		HostIP:         engine.cfg.IP,
		ReplicaId:      releaseRaftConf.ReplicaId,
		ShardIds:       shardIds,
		RaftAddr:       fmt.Sprintf("%s:%d", engine.cfg.IP, releaseRaftConf.RaftPort),
		GrpcPort:       uint16(releaseRaftConf.GrpcPort),
		MultiGroupSize: releaseRaftConf.MultiGroupSize,
		StorageDir:     raftDir,
		Join:           join,
		InitialMembers: initialMembers,
		Gossip:         releaseRaftConf.Gossip,
		GossipPort:     uint16(releaseRaftConf.RaftGossipConfig.GossipPort),
		GossipSeeds:    releaseRaftConf.RaftGossipConfig.GossipSeeds,
		Metrics:        releaseRaftConf.Metrics,
		GossipConfig: gossip.GossipConfig{
			BindAddress: fmt.Sprintf("%s:%d", engine.cfg.IP, releaseRaftConf.BusinessGossipConfig.GossipPort),
			BindPort:    uint16(releaseRaftConf.BusinessGossipConfig.GossipPort),
			Seeds:       releaseRaftConf.BusinessGossipConfig.GossipSeeds,
		},
	}
}

func (engine *Engine) raftConfigByMetadata(raftDir string, shardIds []uint64,
	metadata *raft.RaftMetadata, cluster *gossip.RaftShardMessage) *raft.RaftConfig {
	return &raft.RaftConfig{
		LogDir:         engine.cfg.LogDir,
		LogLevel:       engine.cfg.LogLevel,
		HostIP:         engine.cfg.IP,
		ReplicaId:      metadata.ReplicaId,
		ShardIds:       shardIds,
		RaftAddr:       fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.RaftPort),
		GrpcPort:       metadata.GrpcPort,
		MultiGroupSize: metadata.MultiGroupSize,
		StorageDir:     raftDir,
		Join:           cluster.Join,
		InitialMembers: cluster.InitialMembers,
		Gossip:         metadata.Gossip,
		GossipPort:     metadata.GossipPort,
		GossipSeeds:    metadata.GossipSeeds,
		Metrics:        metadata.Metrics,
		GossipConfig: gossip.GossipConfig{
			BindAddress: fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.GossipConfig.BindPort),
			BindPort:    uint16(metadata.GossipConfig.BindPort),
			Seeds:       metadata.GossipConfig.Seeds,
		},
	}
}

func validate(localIp string, metadata *raft.RaftMetadata, releaseRaftConf *pb.RaftReleaseConfig) error {
	if metadata.MultiGroupSize != releaseRaftConf.MultiGroupSize {
		return fmt.Errorf("raft config [multiGroupSize] changed, %v->%v", metadata.MultiGroupSize, releaseRaftConf.MultiGroupSize)
	}

	if metadata.ReplicaId != releaseRaftConf.ReplicaId {
		return fmt.Errorf("raft config [replicaId] changed, %v->%v", metadata.ReplicaId, releaseRaftConf.ReplicaId)
	}

	if metadata.Gossip != releaseRaftConf.Gossip {
		return fmt.Errorf("raft config [gossip] changed, %v->%v", metadata.Gossip, releaseRaftConf.Gossip)
	}

	if !metadata.Gossip {
		if metadata.LocalIP != localIp {
			return fmt.Errorf("raft config [raftIp] changed, %v->%v", metadata.LocalIP, localIp)
		}

		if metadata.RaftPort != uint16(releaseRaftConf.RaftPort) {
			return fmt.Errorf("raft config [raftPort] changed, %v->%v", metadata.RaftPort, releaseRaftConf.RaftPort)
		}
	}

	return nil
}

func MetadataByRelease(localIp string, releaseRaftConf *pb.RaftReleaseConfig) *raft.RaftMetadata {
	return &raft.RaftMetadata{
		MultiGroupSize: releaseRaftConf.MultiGroupSize,
		ReplicaId:      releaseRaftConf.ReplicaId,
		LocalIP:        localIp,
		RaftPort:       uint16(releaseRaftConf.RaftPort),
		GrpcPort:       uint16(releaseRaftConf.GrpcPort),
		Gossip:         releaseRaftConf.Gossip,
		Revision:       releaseRaftConf.Revision,
		GossipPort:     uint16(releaseRaftConf.RaftGossipConfig.GossipPort),
		GossipSeeds:    releaseRaftConf.RaftGossipConfig.GossipSeeds,
		Metrics:        releaseRaftConf.Metrics,
		GossipConfig: gossip.GossipConfig{
			BindAddress: fmt.Sprintf("%s:%d", localIp, releaseRaftConf.BusinessGossipConfig.GossipPort),
			BindPort:    uint16(releaseRaftConf.BusinessGossipConfig.GossipPort),
			Seeds:       releaseRaftConf.BusinessGossipConfig.GossipSeeds,
		},
	}
}
