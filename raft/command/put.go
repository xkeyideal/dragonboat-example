package command

import (
	"context"
	"encoding/binary"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type PutCommand struct {
	CfName string
	Key    []byte
	Value  []byte

	resp []byte
}

func NewPutCommand(cfName string, key, value []byte) *PutCommand {
	return &PutCommand{CfName: cfName, Key: key, Value: value}
}

func (c *PutCommand) Linear() bool {
	return true
}

func (c *PutCommand) GetType() CommandType {
	return PUT
}

func (c *PutCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	resp, err := syncWrite(ctx, nh, session, c)
	c.resp = resp
	return err
}

func (c *PutCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	wo := mergeWriteOptions(opts...)

	batch := s.Batch()
	defer batch.Close()

	cf := s.GetColumnFamily(c.CfName)

	revisionValue := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionValue, wo.Revision)

	// set revision
	batch.Set(s.BuildColumnFamilyKey(cf, buildRevisionKey(c.Key)), revisionValue, s.GetWo())

	batch.Set(s.BuildColumnFamilyKey(cf, c.Key), c.Value, s.GetWo())
	c.resp = revisionValue

	return s.Write(batch)
}

func (c *PutCommand) GetResp() []byte {
	return c.resp
}
