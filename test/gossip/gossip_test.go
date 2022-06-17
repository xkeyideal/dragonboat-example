package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	"github.com/xkeyideal/dragonboat-example/v3/tools"

	"github.com/hashicorp/go-msgpack/codec"
	"go.uber.org/zap/zapcore"
)

func WriteShardToFile(shard *gossip.RaftShardMessage) error {
	return nil
}

var (
	logDir = "/tmp/logs"
	level  = zapcore.DebugLevel

	gossipShards = []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	seed       = []uint16{8001, 8002, 8003}
	namePrefix = "gossip-test-"

	membership = &gossip.RaftMembershipMessage{
		MemberInfos: map[uint64]*gossip.MemberInfo{
			1: {
				ShardId:        1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "127.0.0.1:9001",
					10002: "127.0.0.1:9002",
					10003: "127.0.0.1:9003",
				},
				LeaderId:    10001,
				LeaderValid: true,
			},
			2: {
				ShardId:        1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "127.0.0.1:9001",
					10002: "127.0.0.1:9002",
					10003: "127.0.0.1:9003",
				},
				LeaderId:    10002,
				LeaderValid: true,
			},
			3: {
				ShardId:        1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "127.0.0.1:9001",
					10002: "127.0.0.1:9002",
					10004: "127.0.0.1:9004",
				},
				LeaderId:    10004,
				LeaderValid: true,
			},
			4: {
				ShardId:        1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10002: "127.0.0.1:9002",
					10003: "127.0.0.1:9003",
					10004: "127.0.0.1:9004",
				},
				LeaderId:    10003,
				LeaderValid: true,
			},
		},
	}

	shardMessage = &gossip.RaftShardMessage{
		Revision: 1,
		Targets: map[string]gossip.TargetShardId{
			"raft-1": {
				GrpcAddr: "127.0.0.1:6001",
				ShardIds: []uint64{1, 2, 3},
			},
			"raft-2": {
				GrpcAddr: "127.0.0.1:6002",
				ShardIds: []uint64{1, 2, 3, 4},
			},
			"raft-3": {
				GrpcAddr: "127.0.0.1:6003",
				ShardIds: []uint64{1, 2, 4},
			},
			"raft-4": {
				GrpcAddr: "127.0.0.1:6004",
				ShardIds: []uint64{3, 4},
			},
		},
		Shards: map[uint64][]string{
			1: {"raft-1", "raft-2", "raft-3"},
			2: {"raft-1", "raft-2", "raft-3"},
			3: {"raft-1", "raft-2", "raft-4"},
			4: {"raft-2", "raft-3", "raft-4"},
		},
	}
)

func TestGossip(t *testing.T) {
	shards := []*gossip.GossipManager{}
	for i := 0; i < 3; i++ {
		cfg := gossip.GossipConfig{
			BindAddress: fmt.Sprintf("0.0.0.0:%d", seed[i]),
			BindPort:    seed[i],
			Seeds:       gossipShards,
		}

		cfg.SetShardCallback(WriteShardToFile)

		name := namePrefix + fmt.Sprintf("%d", seed[i])
		moveToGrpcAddr := shardMessage.Targets[fmt.Sprintf("raft-%d", i+1)].GrpcAddr
		gossipOpts := gossip.GossipOptions{
			Name:               name,
			MoveToGrpcAddr:     moveToGrpcAddr,
			LogDir:             logDir,
			LogLevel:           level,
			DisableCoordinates: true,
		}
		shard, err := gossip.NewGossipManager(cfg, gossipOpts)
		if err != nil {
			t.Fatal(err)
		}

		shard.SetNodeMeta(gossip.Meta{
			MoveToGrpcAddr: moveToGrpcAddr,
			ServerGrpcAddr: moveToGrpcAddr,
		})

		shards = append(shards, shard)
	}

	// 初始化原始的数据状态
	for _, shard := range shards {
		shard.UpdateMembershipMessage(membership)
		shard.UpdateShardMessage(shardMessage)
		time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	}

	// 查询初始化的数据是否与期望的相同
	cm := shards[1].GetShardMessage()
	if !reflect.DeepEqual(cm, shardMessage) {
		t.Fatalf("init shard message unexpected, %v, %v", cm, shardMessage)
	}

	t.Logf("=================update=======================")

	cm.Revision += 1
	cm.Shards = map[uint64][]string{
		1: {"raft-1", "raft-2", "raft-3"},
		2: {"raft-1", "raft-2", "raft-3", "raft-5"},
		3: {"raft-1", "raft-2", "raft-4"},
		4: {"raft-1", "raft-2", "raft-3", "raft-4", "raft-5"},
	}
	shards[2].UpdateShardMessage(cm)

	for retry := 0; retry < 3; retry++ {
		newCm := shards[0].GetShardMessage()
		if !reflect.DeepEqual(cm, newCm) && retry == 3 {
			t.Fatalf("update shard message unexpected, %v, %v", cm, newCm)
		}
		time.Sleep(2 * time.Second)
	}

	time.Sleep(2 * time.Second)
	t.Logf("==================add shard======================")

	cfg := gossip.GossipConfig{
		BindAddress: fmt.Sprintf("0.0.0.0:%d", 8004),
		BindPort:    8004,
		Seeds:       gossipShards,
	}
	cfg.SetShardCallback(WriteShardToFile)
	moveToGrpcAddr := shardMessage.Targets["raft-4"].GrpcAddr
	gossipOpts := gossip.GossipOptions{
		Name:               namePrefix + "8004",
		MoveToGrpcAddr:     moveToGrpcAddr,
		LogDir:             logDir,
		LogLevel:           level,
		DisableCoordinates: true,
	}
	shard4, err := gossip.NewGossipManager(cfg, gossipOpts)
	if err != nil {
		t.Fatal(err)
	}
	shard4.SetNodeMeta(gossip.Meta{
		MoveToGrpcAddr: moveToGrpcAddr,
		ServerGrpcAddr: moveToGrpcAddr,
	})

	shards = append(shards, shard4)
	// shard4.UpdateMembershipMessage(membership)
	// shard4.UpdateShardMessage(shardMessage)

	// 查询初始化的数据是否与期望的相同
	cm4 := shard4.GetShardMessage()
	if !reflect.DeepEqual(cm4, cm) {
		t.Fatalf("get init shard4 message unexpected, %v, %v", cm4, shardMessage)
	}

	time.Sleep(4 * time.Second)

	// 等待一段时间后, gossip协议应该会同步之前集群的数据
	cm4 = shard4.GetShardMessage()
	if !reflect.DeepEqual(cm4, cm) {
		t.Fatalf("get gossip sync shard4 message unexpected, %v, %v", cm4, cm)
	}

	t.Logf("==================add shard down======================")

	k := 1

	t.Log(shards[k].GetAliveInstances())

	shards[k].Close()

	time.Sleep(2 * time.Second)

	t.Log(shards[k+1].GetAliveInstances())

	shards = append(shards[:k], shards[k+1:]...)

	t.Logf("==================close shard down======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	// free
	for _, shard := range shards {
		shard.Close()
	}
}

func TestGossipCoordinate(t *testing.T) {
	shards := []*gossip.GossipManager{}
	for i := 0; i < 3; i++ {
		cfg := gossip.GossipConfig{
			BindAddress: fmt.Sprintf("0.0.0.0:%d", seed[i]),
			BindPort:    seed[i],
			Seeds:       gossipShards,
		}

		cfg.SetShardCallback(WriteShardToFile)

		name := namePrefix + fmt.Sprintf("%d", seed[i])
		moveToGrpcAddr := shardMessage.Targets[fmt.Sprintf("raft-%d", i+1)].GrpcAddr
		gossipOpts := gossip.GossipOptions{
			Name:               name,
			MoveToGrpcAddr:     moveToGrpcAddr,
			LogDir:             logDir,
			LogLevel:           level,
			DisableCoordinates: false,
		}
		shard, err := gossip.NewGossipManager(cfg, gossipOpts)
		if err != nil {
			t.Fatal(err)
		}

		shard.SetNodeMeta(gossip.Meta{
			MoveToGrpcAddr: moveToGrpcAddr,
			ServerGrpcAddr: moveToGrpcAddr,
		})

		shards = append(shards, shard)
	}

	// 初始化原始的数据状态
	for _, shard := range shards {
		shard.UpdateMembershipMessage(membership)
		shard.UpdateShardMessage(shardMessage)
		time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	}

	// 查询初始化的数据是否与期望的相同
	cm := shards[1].GetShardMessage()
	if !reflect.DeepEqual(cm, shardMessage) {
		t.Fatalf("init shard message unexpected, %v, %v", cm, shardMessage)
	}

	t.Logf("=================update=======================")

	// Make sure both nodes start out the origin so we can prove they did
	// an update later.
	c1, err := shards[0].GetCoordinate()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c2, err := shards[1].GetCoordinate()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	const zeroThreshold = 20.0e-6
	if c1.DistanceTo(c2).Seconds() < zeroThreshold {
		t.Fatalf("coordinates didn't update after probes")
	}

	shards[1].Close()

	shards = append(shards[:1], shards[2:]...)

	t.Log(shards[0].GetCachedCoordinate(namePrefix + fmt.Sprintf("%d", seed[1])))

	t.Logf("=================down=======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	// free
	for _, shard := range shards {
		shard.Close()
	}
}

func TestGzipMsgpack(t *testing.T) {
	str := `{
		"revision": 2,
		"targets": {
			"nhid-11772876509704": {
				"grpcAddr": "127.0.0.1:24000",
				"shardIds": [
					0,
					1,
					2,
					3,
					4
				]
			},
			"nhid-11772876509705": {
				"grpcAddr": "127.0.0.1:24001",
				"shardIds": [
					0,
					1,
					5,
					6,
					7,
					2,
					3,
					4
				]
			},
			"nhid-11772876509706": {
				"grpcAddr": "127.0.0.1:24002",
				"shardIds": [
					2,
					3,
					5,
					6
				]
			},
			"nhid-11772876509707": {
				"grpcAddr": "127.0.0.1:24003",
				"shardIds": [
					0,
					2,
					4,
					5,
					7
				]
			},
			"nhid-11772876509708": {
				"grpcAddr": "127.0.0.1:24004",
				"shardIds": [
					1,
					3,
					4,
					6,
					7
				]
			}
		},
		"shards": {
			"0": [
				"nhid-11772876509704",
				"nhid-11772876509705",
				"nhid-11772876509707"
			],
			"1": [
				"nhid-11772876509704",
				"nhid-11772876509705",
				"nhid-11772876509708"
			],
			"2": [
				"nhid-11772876509704",
				"nhid-11772876509707",
				"nhid-11772876509706",
				"nhid-11772876509705"
			],
			"3": [
				"nhid-11772876509704",
				"nhid-11772876509708",
				"nhid-11772876509706",
				"nhid-11772876509705"
			],
			"4": [
				"nhid-11772876509704",
				"nhid-11772876509707",
				"nhid-11772876509708",
				"nhid-11772876509705"
			],
			"5": [
				"nhid-11772876509705",
				"nhid-11772876509707",
				"nhid-11772876509706"
			],
			"6": [
				"nhid-11772876509705",
				"nhid-11772876509708",
				"nhid-11772876509706"
			],
			"7": [
				"nhid-11772876509705",
				"nhid-11772876509707",
				"nhid-11772876509708"
			]
		},
		"initial_members": {
			"0": {
				"11772876509704": "nhid-11772876509704",
				"11772876509705": "nhid-11772876509705",
				"11772876509707": "nhid-11772876509707"
			},
			"1": {
				"11772876509704": "nhid-11772876509704",
				"11772876509705": "nhid-11772876509705",
				"11772876509708": "nhid-11772876509708"
			},
			"2": {
				"11772876509704": "nhid-11772876509704",
				"11772876509706": "nhid-11772876509706",
				"11772876509707": "nhid-11772876509707"
			},
			"3": {
				"11772876509704": "nhid-11772876509704",
				"11772876509706": "nhid-11772876509706",
				"11772876509708": "nhid-11772876509708"
			},
			"4": {
				"11772876509704": "nhid-11772876509704",
				"11772876509707": "nhid-11772876509707",
				"11772876509708": "nhid-11772876509708"
			},
			"5": {
				"11772876509705": "nhid-11772876509705",
				"11772876509706": "nhid-11772876509706",
				"11772876509707": "nhid-11772876509707"
			},
			"6": {
				"11772876509705": "nhid-11772876509705",
				"11772876509706": "nhid-11772876509706",
				"11772876509708": "nhid-11772876509708"
			},
			"7": {
				"11772876509705": "nhid-11772876509705",
				"11772876509707": "nhid-11772876509707",
				"11772876509708": "nhid-11772876509708"
			}
		},
		"join": {
			"0": {
				"11772876509704": false,
				"11772876509705": false,
				"11772876509707": false
			},
			"1": {
				"11772876509704": false,
				"11772876509705": false,
				"11772876509708": false
			},
			"2": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509706": false,
				"11772876509707": false
			},
			"3": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509706": false,
				"11772876509708": false
			},
			"4": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509707": false,
				"11772876509708": false
			},
			"5": {
				"11772876509705": false,
				"11772876509706": false,
				"11772876509707": false
			},
			"6": {
				"11772876509705": false,
				"11772876509706": false,
				"11772876509708": false
			},
			"7": {
				"11772876509705": false,
				"11772876509707": false,
				"11772876509708": false
			}
		}
	}`

	shard := &gossip.RaftShardMessage{}
	err := json.Unmarshal([]byte(str), shard)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(1))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err = encoder.Encode(shard)
	if err != nil {
		t.Fatal(err)
	}

	b := buf.Bytes()

	len1 := len(b)

	gb, err := tools.GZipEncode(b)
	if err != nil {
		t.Fatal(err)
	}

	len2 := len(gb)

	t.Logf("json length: %d, msgpack length: %d, gzip length: %d", len(str), len1, len2)

	bbuf, err := tools.GZipDecode(gb)
	if err != nil {
		t.Fatal(err)
	}

	var handle2 codec.MsgpackHandle
	out := &gossip.RaftShardMessage{}
	err = codec.NewDecoder(bytes.NewReader(bbuf[1:]), &handle2).Decode(out)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(out)
}
