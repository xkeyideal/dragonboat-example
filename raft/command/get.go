package command

import (
	"context"
	"encoding/binary"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type GetCommand struct {
	CfName       string
	Key          []byte
	Linearizable bool
	resp         []byte
}

func NewGetCommand(cfName string, key []byte, linearizable bool) *GetCommand {
	return &GetCommand{
		CfName:       cfName,
		Key:          key,
		Linearizable: linearizable,
	}
}

func (c *GetCommand) Linear() bool {
	return c.Linearizable
}

func (c *GetCommand) GetType() CommandType {
	return GET
}

func (c *GetCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, shardId uint64, _ *client.Session) (err error) {
	c.resp, err = syncRead(ctx, nh, shardId, c)
	return err
}

func (c *GetCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	cf := s.GetColumnFamily(c.CfName)

	// get revision
	v, err := s.GetBytes(s.BuildColumnFamilyKey(cf, buildRevisionKey(c.Key)))
	if err != nil {
		return err
	}

	if len(v) == 0 {
		v = make([]byte, 8)
		binary.BigEndian.PutUint64(v, 0)
	}

	// get value
	d, err := s.GetBytes(s.BuildColumnFamilyKey(cf, c.Key))
	if err != nil {
		return err
	}

	c.resp = append(v, d...)
	return nil
}

func (c *GetCommand) GetResp() []byte {
	return c.resp
}
