package command

import (
	"bytes"
	"context"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type SearchCommand struct {
	CfName       string
	Prefix       []byte
	Linearizable bool
	resp         []byte
}

func NewSearchCommand(cfName string, prefix []byte, linearizable bool) *SearchCommand {
	return &SearchCommand{
		CfName:       cfName,
		Prefix:       prefix,
		Linearizable: linearizable,
	}
}

func (c *SearchCommand) Linear() bool {
	return c.Linearizable
}

func (c *SearchCommand) GetType() CommandType {
	return SEARCH
}

func (c *SearchCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, shardId uint64, _ *client.Session) error {
	result, err := syncRead(ctx, nh, shardId, c)
	if err != nil {
		return err
	}
	c.resp = result
	return nil
}

func (c *SearchCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	cf := s.GetColumnFamily(c.CfName)

	iter := s.GetIterator()
	defer iter.Close()

	resp := [][]byte{}
	for iter.SeekGE(s.BuildColumnFamilyKey(cf, c.Prefix)); iter.Valid(); iter.Next() {
		if iter.Error() != nil {
			return iter.Error()
		}

		// 因为key的第一个位是columnfamily
		key := make([]byte, len(iter.Key())-1)
		copy(key, iter.Key()[1:])

		// pebble存储是顺序的，此处按前缀查找，不使用SeekPrefixGE而使用SeekGE替代
		// 然后通过HasPrefix进行判断，不满足条件则可以直接退出
		if !bytes.HasPrefix(key, c.Prefix) {
			break
		}

		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())

		resp = append(resp, key)
		resp = append(resp, val)
	}

	c.resp = Encode(resp)
	return nil
}

func (c *SearchCommand) GetResp() []byte {
	return c.resp
}
