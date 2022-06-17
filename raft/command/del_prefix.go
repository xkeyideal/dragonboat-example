package command

import (
	"context"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type DelPrefixCommand struct {
	CfName string
	Prefix []byte
}

func NewDelPrefixCommand(cfName string, prefix []byte) *DelPrefixCommand {
	return &DelPrefixCommand{
		CfName: cfName,
		Prefix: prefix,
	}
}

func (c *DelPrefixCommand) Linear() bool {
	return true
}

func (c *DelPrefixCommand) GetType() CommandType {
	return DELETEPREFIX
}

func (c *DelPrefixCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	_, err := syncWrite(ctx, nh, session, c)
	return err
}

func (c *DelPrefixCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	batch := s.Batch()
	defer batch.Close()

	cf := s.GetColumnFamily(c.CfName)

	// del revision
	revisionStartKey := buildRevisionKey(c.Prefix)
	revisionEndKey := increaseOne(revisionStartKey)
	batch.DeleteRange(s.BuildColumnFamilyKey(cf, revisionStartKey), s.BuildColumnFamilyKey(cf, revisionEndKey), s.GetWo())

	// del key
	startKey := c.Prefix
	endKey := increaseOne(startKey)
	batch.DeleteRange(s.BuildColumnFamilyKey(cf, startKey), s.BuildColumnFamilyKey(cf, endKey), s.GetWo())
	return s.Write(batch)
}

func (c *DelPrefixCommand) GetResp() []byte {
	return []byte{}
}

func increaseOne(bytes []byte) []byte {
	length := len(bytes)
	if length == 0 {
		return []byte{}
	}
	var newByte = make([]byte, length)
	copy(newByte, bytes)

	for i := length - 1; i >= 0; i-- {
		newByte[i] += 1
		if newByte[i] != 0 {
			return newByte
		}
	}
	return newByte
}
