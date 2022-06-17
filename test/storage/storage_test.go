package storage

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	"github.com/xkeyideal/dragonboat-example/v3/gossip"
	"github.com/xkeyideal/dragonboat-example/v3/raft"

	"go.uber.org/zap/zapcore"
)

var (
	logDir     = "/tmp/logs"
	storageDir = "/tmp/raft"
	level      = zapcore.DebugLevel

	// 5台机器，总共分为8个raft groups，每个raft group 3台机器
	groupNumber  = 8
	groupMachine = 3

	hostIP = "127.0.0.1"

	raftAddrs = []string{
		"127.0.0.1:21000",
		"127.0.0.1:21001",
		"127.0.0.1:21002",
		"127.0.0.1:21003",
		"127.0.0.1:21004",
	}

	raftGossipPorts = []uint16{
		22000,
		22001,
		22002,
		22003,
		22004,
	}

	raftGossipAddrs = []string{
		"127.0.0.1:22000",
		"127.0.0.1:22001",
		"127.0.0.1:22002",
		"127.0.0.1:22003",
		"127.0.0.1:22004",
	}

	// raft地址对应的moveTo grpc端口号
	grpcPorts = map[string]uint16{
		"127.0.0.1:21000": 24000,
		"127.0.0.1:21001": 24001,
		"127.0.0.1:21002": 24002,
		"127.0.0.1:21003": 24003,
		"127.0.0.1:21004": 24004,
	}

	// 同步shard信息的gossip
	gossipAddrs = []string{
		"127.0.0.1:23000",
		"127.0.0.1:23001",
		"127.0.0.1:23002",
		"127.0.0.1:23003",
		"127.0.0.1:23004",
	}

	// key: addr, val: shardIds
	replicaMap map[string][]uint64
)

func init2() {
	replicaMap = generateRaftShards()
}

func newTestStorage(addr string, cfg *raft.RaftConfig) *raft.Storage {
	s, err := raft.NewStorage(addr, "", cfg)
	if err != nil {
		panic(err)
	}

	return s
}

func newGossipStorage() ([]*raft.Storage, *gossip.RaftShardMessage) {
	init2()
	initRcm := initShardMessage(replicaMap)
	// initialMembers := initialMembers(true)

	storages := []*raft.Storage{}
	wg := sync.WaitGroup{}
	wg.Add(len(raftAddrs))
	for i, addr := range raftAddrs {
		go func(i int, addr string) {
			replicaId := addr2RaftReplicaID(addr)
			cfg := &raft.RaftConfig{
				LogDir:         logDir,
				LogLevel:       level,
				HostIP:         hostIP,
				ReplicaId:      replicaId,
				ShardIds:       replicaMap[addr],
				RaftAddr:       addr,
				GrpcPort:       grpcPorts[addr],
				StorageDir:     storageDir,
				MultiGroupSize: uint32(groupNumber),
				Join:           initRcm.Join,
				InitialMembers: initRcm.InitialMembers,
				Gossip:         true,
				GossipPort:     raftGossipPorts[i],
				GossipSeeds:    raftGossipAddrs,
				Metrics:        false,
				GossipConfig: gossip.GossipConfig{
					BindAddress: gossipAddrs[i],
					Seeds:       gossipAddrs,
				},
			}

			b, _ := json.MarshalIndent(cfg, "", "    ")
			log.Printf("[INFO] generate raft config:\n%s\n", string(b))

			s := newTestStorage(addr, cfg)
			storages = append(storages, s)
			time.Sleep(time.Duration(i+1) * time.Second)
			wg.Done()
		}(i, addr)
	}

	wg.Wait()

	return storages, initRcm
}

// TestStorageByGossip raft采用gossip的方式寻址
func TestStorageByGossip(t *testing.T) {
	storages, initRcm := newGossipStorage()

	sort.Slice(storages, func(i, j int) bool {
		return storages[i].GetReplicaId() < storages[j].GetReplicaId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadShardFromFile(storageDir, s.GetReplicaId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateShardMessage(rcm)
		//s.WriteShardToFile(rcm)
	}

	//t.Log(storages[2])

	t.Logf("==================storage down======================")

	// 测试gossip方式获取NodeHost, 测试结果：在gossip方式启动下
	// dragonboat framework nh.GetNodeHostInfo()函数返回的值是NodeHostID
	//  GetNodeHost map[
	// 11772876509704:nhid-11772876509704
	// 11772876509705:nhid-11772876509705
	// 11772876509706:nhid-11772876509706
	// 11772876509707:nhid-11772876509707
	// 11772876509708:nhid-11772876509708]

	// 新方法采用使用gossip meta数据同步server对外grpcAddr的方式
	t.Log("GetNodeHost", storages[2].GetNodeHost())

	putrevision, err := testRaftPut(storages[2])
	if err != nil {
		t.Fatal(err)
	}

	t.Log("test raft put revision", putrevision)

	getrevision, val, err := testRaftGet(storages[2])
	if err != nil {
		t.Fatal(err)
	}

	if putrevision != getrevision {
		t.Fatalf("put get revision unexpected put: %v, get: %v", putrevision, getrevision)
	}

	if string(val) != "test_raft_put_command_val" {
		t.Fatalf("put get val unexpected put: test_raft_put_command_val, get: %v", string(val))
	}

	t.Logf("==================storage test get put down======================")

	// 测试moveTo查询
	shardId := getShardId("test_raft_put_command")
	var moveToReplicaId uint64 = 0
	for addr, shardIds := range replicaMap {
		ok := true
		for _, id := range shardIds {
			if id == shardId {
				ok = false
				break
			}
		}

		if ok {
			moveToReplicaId = addr2RaftReplicaID(addr)
		}
	}

	var moveToStorage *raft.Storage
	for _, s := range storages {
		if s.GetReplicaId() == moveToReplicaId {
			moveToStorage = s
			break
		}
	}

	getrevision, val, err = testRaftGet(moveToStorage)
	if err != nil {
		t.Fatal(err)
	}

	if putrevision != getrevision {
		t.Fatalf("moveTo put get revision unexpected put: %v, get: %v", putrevision, getrevision)
	}

	if string(val) != "test_raft_put_command_val" {
		t.Fatalf("moveTo put get val unexpected put: test_raft_put_command_val, get: %v", string(val))
	}

	t.Logf("==================storage test moveTo down======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	for _, s := range storages {
		s.StopRaftNode()
	}
}

func TestRaftNodeJoin(t *testing.T) {
	storages, initRcm := newGossipStorage()

	sort.Slice(storages, func(i, j int) bool {
		return storages[i].GetReplicaId() < storages[j].GetReplicaId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadShardFromFile(storageDir, s.GetReplicaId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateShardMessage(rcm)
	}

	time.Sleep(5 * time.Second)

	t.Logf("==================storage down======================")

	// 取出待测试的storages[1]的集群分部情况
	s1Shard := storages[1].GetShardMessage()
	joinshardIds := []uint64{}
	set := make(map[uint64]struct{})
	t.Log(storages[1].GetTarget(), s1Shard.Targets[storages[1].GetTarget()].ShardIds)
	for _, shardId := range s1Shard.Targets[storages[1].GetTarget()].ShardIds {
		set[shardId] = struct{}{}
	}

	// 将没加入的shardIds全部加入storages[1]
	for i := 0; i < groupNumber; i++ {
		if _, ok := set[uint64(i)]; !ok {
			joinshardIds = append(joinshardIds, uint64(i))
		}
	}

	t.Log("args ...interface{}", s1Shard.Revision, joinshardIds)

	// beforeMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ := json.Marshal(beforeMs)
	// t.Log("before membership: ", string(b))

	// 随便找个节点，执行Join操作, 顺带也能执行moveTo
	err := storages[0].AddRaftNode(storages[1].GetReplicaId(), storages[1].GetTarget(), joinshardIds)
	if err != nil {
		t.Fatal(err)
	}

	storages[1].ChangeRaftNodeShardIds(true, joinshardIds)

	time.Sleep(2 * time.Second)

	// 重新拼装storages[1]的集群分配配置
	newShard := DeepCopy(s1Shard).(*gossip.RaftShardMessage)

	newShard.Revision += 1
	target := newShard.Targets[storages[1].GetTarget()]
	target.ShardIds = append(target.ShardIds, joinshardIds...)
	newShard.Targets[storages[1].GetTarget()] = target
	for _, shardId := range joinshardIds {
		newShard.Shards[shardId] = append(newShard.Shards[shardId], storages[1].GetTarget())
		join := newShard.Join[shardId]
		join[storages[1].GetReplicaId()] = true
		newShard.Join[shardId] = join
	}

	// 调用接口，更新集群分配配置
	storages[1].UpdateShardMessage(newShard)

	// sleep 一定时间，便于gossip数据同步收敛
	time.Sleep(5 * time.Second)

	// afterMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ = json.Marshal(afterMs)
	// t.Log("after membership: ", string(b))

	// 此处测试如果不通过，没影响，需要根据gossip的收敛速度来的，目前的收敛速度是秒级
	s2Shard := storages[2].GetShardMessage()
	t.Log("GetShardMessage", storages[2].GetReplicaId(), storages[2].GetTarget(), newShard.Revision, s2Shard.Revision)
	if s2Shard.Revision != newShard.Revision {
		t.Fatalf("shard config unexpected, [%d - %d]", s2Shard.Revision, newShard.Revision)
	}

	t.Logf("==================storage join test down======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	for _, s := range storages {
		s.StopRaftNode()
	}
}

func TestRaftNodeLeave(t *testing.T) {
	storages, initRcm := newGossipStorage()

	sort.Slice(storages, func(i, j int) bool {
		return storages[i].GetReplicaId() < storages[j].GetReplicaId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadShardFromFile(storageDir, s.GetReplicaId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateShardMessage(rcm)
	}

	time.Sleep(5 * time.Second)

	t.Logf("==================storage down======================")

	// 取出待测试的storages[1]的集群分部情况
	s1Shard := storages[1].GetShardMessage()
	leaveshardIds := []uint64{}
	t.Log(storages[1].GetTarget(), s1Shard.Targets[storages[1].GetTarget()].ShardIds)
	for _, shardId := range s1Shard.Targets[storages[1].GetTarget()].ShardIds {
		if shardId%3 == 0 {
			leaveshardIds = append(leaveshardIds, shardId)
		}
	}

	t.Log("args ...interface{}", s1Shard.Revision, leaveshardIds)

	// beforeMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ := json.Marshal(beforeMs)
	// t.Log("before membership: ", string(b))

	// 随便找个节点，执行Leave操作, 顺带也能执行moveTo
	err := storages[0].RemoveRaftNode(storages[1].GetReplicaId(), leaveshardIds)
	if err != nil {
		t.Fatal(err)
	}

	storages[1].ChangeRaftNodeShardIds(false, leaveshardIds)

	time.Sleep(2 * time.Second)

	// 重新拼装storages[1]的集群分配配置
	newShard := DeepCopy(s1Shard).(*gossip.RaftShardMessage)

	newShard.Revision += 1
	target := newShard.Targets[storages[1].GetTarget()]

	set := make(map[uint64]struct{})
	for _, shardId := range target.ShardIds {
		set[shardId] = struct{}{}
	}
	for _, shardId := range leaveshardIds {
		delete(set, shardId)
	}

	shardIds := []uint64{}
	for shardId := range set {
		shardIds = append(shardIds, shardId)
	}
	target.ShardIds = shardIds
	newShard.Targets[storages[1].GetTarget()] = target
	for _, shardId := range leaveshardIds {
		targets := []string{}
		for _, target := range newShard.Shards[shardId] {
			if target == storages[1].GetTarget() {
				continue
			}
			targets = append(targets, target)
		}
		newShard.Shards[shardId] = targets

		join := newShard.Join[shardId]
		delete(join, storages[1].GetReplicaId())
		//join[storages[1].GetReplicaId()] = false
		newShard.Join[shardId] = join

		initialMembers := newShard.InitialMembers[shardId]
		delete(initialMembers, storages[1].GetReplicaId())
		newShard.InitialMembers[shardId] = initialMembers
	}

	// 调用接口，更新集群分配配置
	storages[1].UpdateShardMessage(newShard)

	// sleep 一定时间，便于gossip数据同步收敛
	time.Sleep(5 * time.Second)

	// afterMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ = json.Marshal(afterMs)
	// t.Log("after membership: ", string(b))

	// 此处测试如果不通过，没影响，需要根据gossip的收敛速度来的，目前的收敛速度是秒级
	s2Shard := storages[2].GetShardMessage()
	t.Log("GetShardMessage", storages[2].GetReplicaId(), storages[2].GetTarget(), newShard.Revision, s2Shard.Revision)
	if s2Shard.Revision != newShard.Revision {
		t.Fatalf("shard config unexpected, [%d - %d]", s2Shard.Revision, newShard.Revision)
	}

	t.Logf("==================storage join test down======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	for _, s := range storages {
		s.StopRaftNode()
	}
}

func testRaftPut(s *raft.Storage) (uint64, error) {
	key := "test_raft_put_command"
	val := "test_raft_put_command_val"

	return s.Put(config.ColumnFamilyDefault, key, []byte(key), []byte(val))
}

func testRaftGet(s *raft.Storage) (uint64, []byte, error) {
	key := "test_raft_put_command"

	return s.Get(config.ColumnFamilyDefault, key, false, []byte(key))
}

func initShardMessage(replicaMap map[string][]uint64) *gossip.RaftShardMessage {
	shards := make(map[uint64][]string)
	targets := make(map[string]gossip.TargetShardId)
	m := make(map[string]uint64)
	for addr, shardIds := range replicaMap {
		replicaId := addr2RaftReplicaID(addr)
		targetAddr := fmt.Sprintf("nhid-%d", replicaId)
		m[targetAddr] = replicaId
		for _, shardId := range shardIds {
			shards[shardId] = append(shards[shardId], targetAddr)
		}
		ss := strings.Split(addr, ":")
		targets[targetAddr] = gossip.TargetShardId{
			GrpcAddr: fmt.Sprintf("%s:%d", ss[0], grpcPorts[addr]),
			ShardIds: shardIds,
		}
	}

	initialMembers := make(map[uint64]map[uint64]string)
	join := make(map[uint64]map[uint64]bool)
	for shardId, targets := range shards {
		im := make(map[uint64]string)
		jn := make(map[uint64]bool)
		for _, target := range targets {
			im[m[target]] = target
			jn[m[target]] = false
		}

		initialMembers[shardId] = im
		join[shardId] = jn
	}

	initRcm := &gossip.RaftShardMessage{
		Revision:       1,
		Targets:        targets,
		Shards:         shards,
		InitialMembers: initialMembers,
		Join:           join,
	}

	return initRcm
}

func getShardId(hashKey string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(hashKey)) % uint32(groupNumber))
}

func initialMembers(gossip bool) map[uint64]string {
	initialMembers := make(map[uint64]string)
	for _, addr := range raftAddrs {
		replicaId := addr2RaftReplicaID(addr)
		if gossip {
			initialMembers[replicaId] = fmt.Sprintf("nhid-%d", replicaId)
		} else {
			initialMembers[replicaId] = addr
		}
	}

	return initialMembers
}

func generateRaftShards() map[string][]uint64 {
	groups := combination(raftAddrs, groupMachine)
	n := len(groups)
	skip := (n - groupNumber) / 2

	shards := [][]string{}
	for i := 0; i < groupNumber; i++ {
		shards = append(shards, groups[i+skip])
	}

	replicaMap := make(map[string][]uint64)
	for id, shard := range shards {
		for _, addr := range shard {
			replicaMap[addr] = append(replicaMap[addr], uint64(id))
		}
	}

	return replicaMap
}

// [
// 	[127.0.0.1:21000 127.0.0.1:21001 127.0.0.1:21002]
// 	[127.0.0.1:21000 127.0.0.1:21001 127.0.0.1:21003]
// 	[127.0.0.1:21000 127.0.0.1:21001 127.0.0.1:21004]
// 	[127.0.0.1:21000 127.0.0.1:21002 127.0.0.1:21003]
// 	[127.0.0.1:21000 127.0.0.1:21002 127.0.0.1:21004]
// 	[127.0.0.1:21000 127.0.0.1:21003 127.0.0.1:21004]
// 	[127.0.0.1:21001 127.0.0.1:21002 127.0.0.1:21003]
// 	[127.0.0.1:21001 127.0.0.1:21002 127.0.0.1:21004]
// 	[127.0.0.1:21001 127.0.0.1:21003 127.0.0.1:21004]
// 	[127.0.0.1:21002 127.0.0.1:21003 127.0.0.1:21004]
//  ]
func combination(arrs []string, k int) [][]string {
	sort.Slice(arrs, func(i, j int) bool {
		return arrs[i] < arrs[j]
	})

	ans, res := []string{}, [][]string{}

	helper(arrs, 0, k, len(arrs), ans, &res)

	return res
}

func helper(arrs []string, start, k, n int, ans []string, res *[][]string) {
	if len(ans) == k {
		tmp := make([]string, k)
		copy(tmp, ans)
		*res = append(*res, tmp)
		return
	}

	for i := start; i < n; i++ {
		if len(ans) >= k {
			break
		}

		ans = append(ans, arrs[i])
		helper(arrs, i+1, k, n, ans, res)
		ans = ans[:len(ans)-1]
	}
}

func addr2RaftReplicaID(addr string) uint64 {
	s := strings.Split(addr, ":")
	bits := strings.Split(s[0], ".")

	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])

	var sum uint64

	sum += uint64(b0) << 24
	sum += uint64(b1) << 16
	sum += uint64(b2) << 8
	sum += uint64(b3)

	port, _ := strconv.Atoi(s[1])

	sum = sum<<16 + uint64(port)

	return sum
}
