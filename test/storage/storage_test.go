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

	// 同步cluster信息的gossip
	gossipAddrs = []string{
		"127.0.0.1:23000",
		"127.0.0.1:23001",
		"127.0.0.1:23002",
		"127.0.0.1:23003",
		"127.0.0.1:23004",
	}

	// key: addr, val: clusterIds
	nodeMap map[string][]uint64
)

func init2() {
	nodeMap = generateRaftClusters()
}

func newTestStorage(addr string, cfg *raft.RaftConfig) *raft.Storage {
	s, err := raft.NewStorage(addr, "", cfg)
	if err != nil {
		panic(err)
	}

	return s
}

func newGossipStorage() ([]*raft.Storage, *gossip.RaftClusterMessage) {
	init2()
	initRcm := initClusterMessage(nodeMap)
	// initialMembers := initialMembers(true)

	storages := []*raft.Storage{}
	wg := sync.WaitGroup{}
	wg.Add(len(raftAddrs))
	for i, addr := range raftAddrs {
		go func(i int, addr string) {
			nodeId := addr2RaftNodeID(addr)
			cfg := &raft.RaftConfig{
				LogDir:         logDir,
				LogLevel:       level,
				HostIP:         hostIP,
				NodeId:         nodeId,
				ClusterIds:     nodeMap[addr],
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
		return storages[i].GetNodeId() < storages[j].GetNodeId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadClusterFromFile(storageDir, s.GetNodeId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateClusterMessage(rcm)
		//s.WriteClusterToFile(rcm)
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
	clusterId := getClusterId("test_raft_put_command")
	var moveToNodeId uint64 = 0
	for addr, clusterIds := range nodeMap {
		ok := true
		for _, id := range clusterIds {
			if id == clusterId {
				ok = false
				break
			}
		}

		if ok {
			moveToNodeId = addr2RaftNodeID(addr)
		}
	}

	var moveToStorage *raft.Storage
	for _, s := range storages {
		if s.GetNodeId() == moveToNodeId {
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
		return storages[i].GetNodeId() < storages[j].GetNodeId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadClusterFromFile(storageDir, s.GetNodeId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateClusterMessage(rcm)
	}

	time.Sleep(5 * time.Second)

	t.Logf("==================storage down======================")

	// 取出待测试的storages[1]的集群分部情况
	s1Cluster := storages[1].GetClusterMessage()
	joinClusterIds := []uint64{}
	set := make(map[uint64]struct{})
	t.Log(storages[1].GetTarget(), s1Cluster.Targets[storages[1].GetTarget()].ClusterIds)
	for _, clusterId := range s1Cluster.Targets[storages[1].GetTarget()].ClusterIds {
		set[clusterId] = struct{}{}
	}

	// 将没加入的clusterIds全部加入storages[1]
	for i := 0; i < groupNumber; i++ {
		if _, ok := set[uint64(i)]; !ok {
			joinClusterIds = append(joinClusterIds, uint64(i))
		}
	}

	t.Log("args ...interface{}", s1Cluster.Revision, joinClusterIds)

	// beforeMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ := json.Marshal(beforeMs)
	// t.Log("before membership: ", string(b))

	// 随便找个节点，执行Join操作, 顺带也能执行moveTo
	err := storages[0].AddRaftNode(storages[1].GetNodeId(), storages[1].GetTarget(), joinClusterIds)
	if err != nil {
		t.Fatal(err)
	}

	storages[1].ChangeRaftNodeClusterIds(true, joinClusterIds)

	time.Sleep(2 * time.Second)

	// 重新拼装storages[1]的集群分配配置
	newCluster := DeepCopy(s1Cluster).(*gossip.RaftClusterMessage)

	newCluster.Revision += 1
	target := newCluster.Targets[storages[1].GetTarget()]
	target.ClusterIds = append(target.ClusterIds, joinClusterIds...)
	newCluster.Targets[storages[1].GetTarget()] = target
	for _, clusterId := range joinClusterIds {
		newCluster.Clusters[clusterId] = append(newCluster.Clusters[clusterId], storages[1].GetTarget())
		join := newCluster.Join[clusterId]
		join[storages[1].GetNodeId()] = true
		newCluster.Join[clusterId] = join
	}

	// 调用接口，更新集群分配配置
	storages[1].UpdateClusterMessage(newCluster)

	// sleep 一定时间，便于gossip数据同步收敛
	time.Sleep(5 * time.Second)

	// afterMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ = json.Marshal(afterMs)
	// t.Log("after membership: ", string(b))

	// 此处测试如果不通过，没影响，需要根据gossip的收敛速度来的，目前的收敛速度是秒级
	s2Cluster := storages[2].GetClusterMessage()
	t.Log("GetClusterMessage", storages[2].GetNodeId(), storages[2].GetTarget(), newCluster.Revision, s2Cluster.Revision)
	if s2Cluster.Revision != newCluster.Revision {
		t.Fatalf("cluster config unexpected, [%d - %d]", s2Cluster.Revision, newCluster.Revision)
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
		return storages[i].GetNodeId() < storages[j].GetNodeId()
	})

	t.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadClusterFromFile(storageDir, s.GetNodeId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				t.Fatal(err)
			}
		}

		s.UpdateClusterMessage(rcm)
	}

	time.Sleep(5 * time.Second)

	t.Logf("==================storage down======================")

	// 取出待测试的storages[1]的集群分部情况
	s1Cluster := storages[1].GetClusterMessage()
	leaveClusterIds := []uint64{}
	t.Log(storages[1].GetTarget(), s1Cluster.Targets[storages[1].GetTarget()].ClusterIds)
	for _, clusterId := range s1Cluster.Targets[storages[1].GetTarget()].ClusterIds {
		if clusterId%3 == 0 {
			leaveClusterIds = append(leaveClusterIds, clusterId)
		}
	}

	t.Log("args ...interface{}", s1Cluster.Revision, leaveClusterIds)

	// beforeMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ := json.Marshal(beforeMs)
	// t.Log("before membership: ", string(b))

	// 随便找个节点，执行Leave操作, 顺带也能执行moveTo
	err := storages[0].RemoveRaftNode(storages[1].GetNodeId(), leaveClusterIds)
	if err != nil {
		t.Fatal(err)
	}

	storages[1].ChangeRaftNodeClusterIds(false, leaveClusterIds)

	time.Sleep(2 * time.Second)

	// 重新拼装storages[1]的集群分配配置
	newCluster := DeepCopy(s1Cluster).(*gossip.RaftClusterMessage)

	newCluster.Revision += 1
	target := newCluster.Targets[storages[1].GetTarget()]

	set := make(map[uint64]struct{})
	for _, clusterId := range target.ClusterIds {
		set[clusterId] = struct{}{}
	}
	for _, clusterId := range leaveClusterIds {
		delete(set, clusterId)
	}

	clusterIds := []uint64{}
	for clusterId := range set {
		clusterIds = append(clusterIds, clusterId)
	}
	target.ClusterIds = clusterIds
	newCluster.Targets[storages[1].GetTarget()] = target
	for _, clusterId := range leaveClusterIds {
		targets := []string{}
		for _, target := range newCluster.Clusters[clusterId] {
			if target == storages[1].GetTarget() {
				continue
			}
			targets = append(targets, target)
		}
		newCluster.Clusters[clusterId] = targets

		join := newCluster.Join[clusterId]
		delete(join, storages[1].GetNodeId())
		//join[storages[1].GetNodeId()] = false
		newCluster.Join[clusterId] = join

		initialMembers := newCluster.InitialMembers[clusterId]
		delete(initialMembers, storages[1].GetNodeId())
		newCluster.InitialMembers[clusterId] = initialMembers
	}

	// 调用接口，更新集群分配配置
	storages[1].UpdateClusterMessage(newCluster)

	// sleep 一定时间，便于gossip数据同步收敛
	time.Sleep(5 * time.Second)

	// afterMs, err := storages[1].GetRaftMembership()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// b, _ = json.Marshal(afterMs)
	// t.Log("after membership: ", string(b))

	// 此处测试如果不通过，没影响，需要根据gossip的收敛速度来的，目前的收敛速度是秒级
	s2Cluster := storages[2].GetClusterMessage()
	t.Log("GetClusterMessage", storages[2].GetNodeId(), storages[2].GetTarget(), newCluster.Revision, s2Cluster.Revision)
	if s2Cluster.Revision != newCluster.Revision {
		t.Fatalf("cluster config unexpected, [%d - %d]", s2Cluster.Revision, newCluster.Revision)
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

func initClusterMessage(nodeMap map[string][]uint64) *gossip.RaftClusterMessage {
	clusters := make(map[uint64][]string)
	targets := make(map[string]gossip.TargetClusterId)
	m := make(map[string]uint64)
	for addr, clusterIds := range nodeMap {
		nodeId := addr2RaftNodeID(addr)
		targetAddr := fmt.Sprintf("nhid-%d", nodeId)
		m[targetAddr] = nodeId
		for _, clusterId := range clusterIds {
			clusters[clusterId] = append(clusters[clusterId], targetAddr)
		}
		ss := strings.Split(addr, ":")
		targets[targetAddr] = gossip.TargetClusterId{
			GrpcAddr:   fmt.Sprintf("%s:%d", ss[0], grpcPorts[addr]),
			ClusterIds: clusterIds,
		}
	}

	initialMembers := make(map[uint64]map[uint64]string)
	join := make(map[uint64]map[uint64]bool)
	for clusterId, targets := range clusters {
		im := make(map[uint64]string)
		jn := make(map[uint64]bool)
		for _, target := range targets {
			im[m[target]] = target
			jn[m[target]] = false
		}

		initialMembers[clusterId] = im
		join[clusterId] = jn
	}

	initRcm := &gossip.RaftClusterMessage{
		Revision:       1,
		Targets:        targets,
		Clusters:       clusters,
		InitialMembers: initialMembers,
		Join:           join,
	}

	return initRcm
}

func getClusterId(hashKey string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(hashKey)) % uint32(groupNumber))
}

func initialMembers(gossip bool) map[uint64]string {
	initialMembers := make(map[uint64]string)
	for _, addr := range raftAddrs {
		nodeId := addr2RaftNodeID(addr)
		if gossip {
			initialMembers[nodeId] = fmt.Sprintf("nhid-%d", nodeId)
		} else {
			initialMembers[nodeId] = addr
		}
	}

	return initialMembers
}

func generateRaftClusters() map[string][]uint64 {
	groups := combination(raftAddrs, groupMachine)
	n := len(groups)
	skip := (n - groupNumber) / 2

	clusters := [][]string{}
	for i := 0; i < groupNumber; i++ {
		clusters = append(clusters, groups[i+skip])
	}

	nodeMap := make(map[string][]uint64)
	for id, cluster := range clusters {
		for _, addr := range cluster {
			nodeMap[addr] = append(nodeMap[addr], uint64(id))
		}
	}

	return nodeMap
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

func addr2RaftNodeID(addr string) uint64 {
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
