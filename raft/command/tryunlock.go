package command

import (
	"context"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type TryUnlockCommand struct {
	CfName string
	Key    []byte
	resp   []byte
}

func NewTryUnLockCommand(cfName string, key []byte) *TryUnlockCommand {
	return &TryUnlockCommand{CfName: cfName, Key: key}
}

func (c *TryUnlockCommand) Linear() bool {
	return true
}

func (c *TryUnlockCommand) GetType() CommandType {
	return TRYUNLOCK
}

func (c *TryUnlockCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	data, err := syncWrite(ctx, nh, session, c)
	c.resp = data
	return err
}

func (c *TryUnlockCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	batch := s.Batch()
	defer batch.Close()

	cf := s.GetColumnFamily(c.CfName)
	key := append(lockKey, c.Key...)

	// 查询db，key是否存在
	b, err := s.GetBytes(s.BuildColumnFamilyKey(cf, key))
	if err != nil {
		return err
	}

	// key存在
	if len(b) != 0 {
		batch.Delete(s.BuildColumnFamilyKey(cf, key), s.GetWo())
		if err := s.Write(batch); err != nil {
			return err
		}
		c.resp = []byte{1} // 当前key加锁了，解锁成功
		return nil
	}

	c.resp = []byte{0} // key不存在，表示当前该key没有被加锁
	return nil

}

func (c *TryUnlockCommand) GetResp() []byte {
	return c.resp
}
