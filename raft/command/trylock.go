package command

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type TryLockCommand struct {
	CfName        string
	Key           []byte
	TimeoutSecond uint64
	resp          []byte
}

func NewTryLockCommand(cfName string, key string, second uint64) *TryLockCommand {
	return &TryLockCommand{CfName: cfName, Key: []byte(key), TimeoutSecond: second}
}

func (c *TryLockCommand) Linear() bool {
	return true
}

func (c *TryLockCommand) GetType() CommandType {
	return TRYLOCK
}

func (c *TryLockCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	resp, err := syncWrite(ctx, nh, session, c)
	c.resp = resp
	return err
}

func (c *TryLockCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	batch := s.Batch()
	defer batch.Close()

	timeout := c.TimeoutSecond
	//如果还没设置就已经超时，则直接返回false
	if uint64(time.Now().Unix()) >= timeout {
		c.resp = []byte{0} // 加锁失败
		return nil
	}

	cf := s.GetColumnFamily(c.CfName)

	timeoutBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeoutBytes, c.TimeoutSecond)
	key := append(lockKey, c.Key...)

	// 查询db，需加锁的key是否存在
	b, err := s.GetBytes(s.BuildColumnFamilyKey(cf, key))
	if err != nil {
		return err
	}

	//如果当前key不存在，则保存key，并且返回true
	if len(b) == 0 {
		batch.Set(s.BuildColumnFamilyKey(cf, key), timeoutBytes, s.GetWo())
		if err := s.Write(batch); err != nil {
			return err
		}
		c.resp = []byte{1} // 加锁成功
		return nil
	}

	//如果当前key存在，则检查下当前key的过期时间，如果已经过期，则当他不存在，否则，返回false
	oldTimeout := binary.BigEndian.Uint64(b)
	currentUnix := uint64(time.Now().Unix())

	if currentUnix < oldTimeout {
		c.resp = []byte{0} // 时间未过期，即其他进程对该key进行了加锁，所以返回加锁失败
		return nil
	}

	// 之前该key加锁的时间已经过期了，继续重新加锁
	batch.Set(s.BuildColumnFamilyKey(cf, key), timeoutBytes, s.GetWo())
	if err := s.Write(batch); err != nil {
		return err
	}
	c.resp = []byte{1} // 加锁成功
	return nil
}

func (c *TryLockCommand) GetResp() []byte {
	return c.resp
}
