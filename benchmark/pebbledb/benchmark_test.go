package pebbledb

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/xkeyideal/dragonboat-example/v3/config"
	"github.com/xkeyideal/dragonboat-example/v3/raft/command"
	"github.com/xkeyideal/dragonboat-example/v3/store"
)

// write sync = true
// BenchmarkPebbleDBPut64-8             217           4883446 ns/op
// BenchmarkPebbleDBPut256-8            195           6133421 ns/op
// BenchmarkPebbleDBPut512-8            255           4852964 ns/op
// BenchmarkPebbleDBPut1024-8           252           5156982 ns/op
// BenchmarkPebbleDBPut4096-8           194           5525891 ns/op
// BenchmarkPebbleDBGet-8            632474              1627 ns/op

// write sync = false
// BenchmarkPebbleDBPut64-8          184946              6280 ns/op
// BenchmarkPebbleDBPut256-8         116828             12464 ns/op
// BenchmarkPebbleDBPut512-8          65370             17817 ns/op
// BenchmarkPebbleDBPut1024-8         33733             52638 ns/op
// BenchmarkPebbleDBPut4096-8         11484            103109 ns/op
// BenchmarkPebbleDBGet-8            771906              1502 ns/op

func initPebble() *store.Store {
	dir := "/tmp/pebbledb"
	nodeId := 200000
	clusterId := 10001
	raftPath := filepath.Join(dir, fmt.Sprintf("data_node%d", nodeId))
	os.RemoveAll(raftPath)
	s, err := store.NewStore(uint64(clusterId), filepath.Join(raftPath, strconv.Itoa(int(clusterId))),
		store.PebbleClusterOption{}, nil,
	)
	if err != nil {
		panic(err)
	}

	return s
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

func benchmarkPebbleDBPut(vlen int, b *testing.B) {
	s := initPebble()
	cfName := config.ColumnFamilyDefault

	for i := 0; i < b.N; i++ {
		key := []byte(randomLength(32))
		val := []byte(randomLength(vlen))

		cmd := command.NewPutCommand(cfName, key, val)
		opts := &command.WriteOptions{
			Revision: uint64(i),
		}
		err := cmd.LocalInvoke(s, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPebbleDBPut64(b *testing.B) {
	benchmarkPebbleDBPut(64, b)
}

func BenchmarkPebbleDBPut256(b *testing.B) {
	benchmarkPebbleDBPut(256, b)
}

func BenchmarkPebbleDBPut512(b *testing.B) {
	benchmarkPebbleDBPut(512, b)
}

func BenchmarkPebbleDBPut1024(b *testing.B) {
	benchmarkPebbleDBPut(1024, b)
}

func BenchmarkPebbleDBPut4096(b *testing.B) {
	benchmarkPebbleDBPut(4096, b)
}

func BenchmarkPebbleDBGet(b *testing.B) {
	s := initPebble()
	cfName := config.ColumnFamilyDefault

	prefix := randomLength(32)
	for i := 0; i < 10; i++ {
		key := []byte(prefix + strconv.Itoa(i))
		val := []byte(randomLength(128))

		cmd := command.NewPutCommand(cfName, key, val)
		opts := &command.WriteOptions{
			Revision: uint64(i),
		}
		err := cmd.LocalInvoke(s, opts)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		key := []byte(prefix + strconv.Itoa(i%10))

		cmd := command.NewGetCommand(cfName, key, false)
		err := cmd.LocalInvoke(s)
		if err != nil {
			b.Fatal(err)
		}
	}
}
