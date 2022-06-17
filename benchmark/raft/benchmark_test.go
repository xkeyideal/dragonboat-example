package raft

import (
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	level      = zapcore.ErrorLevel

	// 5台机器，总共分为8个raft groups，每个raft group 3台机器
	groupNumber  = 8
	groupMachine = 3

	hostIP = "127.0.0.1"

	raftAddrs = []string{
		"127.0.0.1:31000",
		"127.0.0.1:31001",
		"127.0.0.1:31002",
		"127.0.0.1:31003",
		"127.0.0.1:31004",
	}

	raftGossipPorts = []uint16{
		32000,
		32001,
		32002,
		32003,
		32004,
	}

	raftGossipAddrs = []string{
		"127.0.0.1:32000",
		"127.0.0.1:32001",
		"127.0.0.1:32002",
		"127.0.0.1:32003",
		"127.0.0.1:32004",
	}

	// raft地址对应的moveTo grpc端口号
	grpcPorts = map[string]uint16{
		"127.0.0.1:31000": 34000,
		"127.0.0.1:31001": 34001,
		"127.0.0.1:31002": 34002,
		"127.0.0.1:31003": 34003,
		"127.0.0.1:31004": 34004,
	}

	// 同步cluster信息的gossip
	gossipAddrs = []string{
		"127.0.0.1:33000",
		"127.0.0.1:33001",
		"127.0.0.1:33002",
		"127.0.0.1:33003",
		"127.0.0.1:33004",
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

			s := newTestStorage(addr, cfg)
			storages = append(storages, s)
			time.Sleep(time.Duration(i+1) * time.Second)
			wg.Done()
		}(i, addr)
	}

	wg.Wait()

	return storages, initRcm
}

type kv struct {
	key       string
	val       []byte
	clusterId uint64
	idx       int
}

// 单线程测试结果:
// pebbledb write sync = false & grpc new connection
// benchmark_test.go:210: raft chaos put 100 loop: 20459902
// benchmark_test.go:249: raft local put 100 loop: 23919508
// benchmark_test.go:182: raft chaos linear: false get 100 loop: 193434
// benchmark_test.go:182: raft chaos linear: true get 100 loop: 345493

// pebbledb write sync = false & grpc connection pool
// benchmark_test.go:212: raft chaos put 100 loop: 20957583
// benchmark_test.go:251: raft local put 100 loop: 20322155
// benchmark_test.go:184: raft chaos linear: false get 100 loop: 107068
// benchmark_test.go:184: raft chaos linear: true get 100 loop: 279758

// 多线程测试结果:
// pebbledb write sync = false & grpc connection pool
// 100 + 1kb
// benchmark_test.go:234: raft chaos put 100 loop: 2535097
// benchmark_test.go:280: raft local put 100 loop: 1865604
// benchmark_test.go:199: raft chaos linear: false get 100 loop: 63551
// benchmark_test.go:199: raft chaos linear: true get 100 loop: 169689
// 1000 + 1kb
// benchmark_test.go:239: raft chaos put 1000 loop: 424605
// benchmark_test.go:285: raft local put 1000 loop: 563444
// benchmark_test.go:204: raft chaos linear: false get 1000 loop: 36888
// benchmark_test.go:204: raft chaos linear: true get 1000 loop: 51898
// 1000 + 10kb
// benchmark_test.go:239: raft chaos put 1000 loop: 1420912
// benchmark_test.go:285: raft local put 1000 loop: 689942
// benchmark_test.go:204: raft chaos linear: false get 1000 loop: 19371
// benchmark_test.go:204: raft chaos linear: true get 1000 loop: 42589

// 每次创建连接与使用连接池对比差距很小，经测试线性操作的99%的耗时在raft的SyncPropose操作上
func benchmarkRaftGetChaos(num, vlen int, linear bool, storages []*raft.Storage, b *testing.B) {
	sl := len(storages)

	kvs := []kv{}
	for i := 0; i < num; i++ {
		v := kv{
			key: randomLength(32),
			val: []byte(randomLength(vlen)),
		}

		v.clusterId = getClusterId(v.key)

		s := storages[i%sl]
		_, err := s.Put(config.ColumnFamilyDefault, v.key, []byte(v.key), v.val)
		if err != nil {
			b.Fatal(err)
		}

		kvs = append(kvs, v)
	}

	wg := sync.WaitGroup{}
	wg.Add(num)

	st := time.Now()
	for i, k := range kvs {
		go func(i int, kv kv) {
			s := storages[i%sl]
			_, _, err := s.Get(config.ColumnFamilyDefault, kv.key, linear, []byte(kv.key))
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(i, k)
	}
	wg.Wait()
	ed := time.Now()

	b.Logf("raft chaos linear: %v get %d loop: %d\n", linear, num, int(ed.Sub(st))/num)
}

func benchmarkRaftPutChaos(num, vlen int, storages []*raft.Storage, b *testing.B) {
	sl := len(storages)

	kvs := []kv{}
	for i := 0; i < num; i++ {
		v := kv{
			key: randomLength(32),
			val: []byte(randomLength(vlen)),
		}

		v.clusterId = getClusterId(v.key)
		kvs = append(kvs, v)
	}

	wg := sync.WaitGroup{}
	wg.Add(num)

	st := time.Now()
	for i, k := range kvs {
		go func(i int, kv kv) {
			s := storages[i%sl]
			_, err := s.Put(config.ColumnFamilyDefault, kv.key, []byte(kv.key), kv.val)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(i, k)
	}

	wg.Wait()
	ed := time.Now()

	b.Logf("raft chaos put %d loop: %d\n", num, int(ed.Sub(st))/num)
}

func benchmarkRaftPutLocal(num, vlen int, storages []*raft.Storage, b *testing.B) {
	clusterMap := make(map[uint64][]int)
	for i, s := range storages {
		for _, clusterId := range s.GetClusterMessage().Targets[s.GetTarget()].ClusterIds {
			clusterMap[clusterId] = append(clusterMap[clusterId], i)
		}
	}

	kvs := []kv{}
	for i := 0; i < num; i++ {
		v := kv{
			key: randomLength(32),
			val: []byte(randomLength(vlen)),
		}

		v.clusterId = getClusterId(v.key)
		idx, ok := clusterMap[v.clusterId]
		if !ok {
			b.Fatalf("clusterId: %d not found storage", v.clusterId)
		}

		v.idx = idx[mrand.Intn(len(idx))]
		kvs = append(kvs, v)
	}

	wg := sync.WaitGroup{}
	wg.Add(num)

	st := time.Now()
	for _, k := range kvs {
		go func(kv kv) {
			s := storages[kv.idx]
			_, err := s.Put(config.ColumnFamilyDefault, kv.key, []byte(kv.key), kv.val)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}(k)
	}

	wg.Wait()
	ed := time.Now()

	b.Logf("raft local put %d loop: %d\n", num, int(ed.Sub(st))/num)
}

func BenchmarkRaftStorage(b *testing.B) {
	storages, initRcm := newGossipStorage()

	sort.Slice(storages, func(i, j int) bool {
		return storages[i].GetNodeId() < storages[j].GetNodeId()
	})

	b.Logf("==================storage start over======================")

	for _, s := range storages {
		rcm, err := raft.ReadClusterFromFile(storageDir, s.GetNodeId())
		if err != nil {
			if err == os.ErrNotExist {
				rcm = initRcm
			} else {
				b.Fatal(err)
			}
		}

		s.UpdateClusterMessage(rcm)
	}

	time.Sleep(2 * time.Second)

	b.Logf("==================storage start up======================")

	benchmarkRaftPutChaos(1000, 1024, storages, b)
	benchmarkRaftPutLocal(1000, 1024, storages, b)
	benchmarkRaftGetChaos(1000, 1024, false, storages, b)
	benchmarkRaftGetChaos(1000, 1024, true, storages, b)

	benchmarkRaftPutChaos(1000, 10240, storages, b)
	benchmarkRaftPutLocal(1000, 10240, storages, b)
	benchmarkRaftGetChaos(1000, 10240, false, storages, b)
	benchmarkRaftGetChaos(1000, 10240, true, storages, b)

	for _, s := range storages {
		s.StopRaftNode()
	}
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

var idChars = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func randomLength(length int) string {
	b := randomBytesMod(length, byte(len(idChars)))
	for i, c := range b {
		b[i] = idChars[c]
	}
	return string(b)
}

func randomBytesMod(length int, mod byte) (b []byte) {
	maxrb := 255 - byte(256%int(mod))
	b = make([]byte, length)
	i := 0
	for {
		r := randomBytes(length + (length / 4))
		for _, c := range r {
			if c > maxrb {
				continue
			}
			b[i] = c % mod
			i++
			if i == length {
				return b
			}
		}
	}
}

func randomBytes(length int) (b []byte) {
	b = make([]byte, length)
	io.ReadFull(rand.Reader, b)
	return
}
