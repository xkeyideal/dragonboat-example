package command

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	"github.com/xkeyideal/dragonboat-example/v3/store"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type CommandType uint8

const (
	DELETE CommandType = iota
	PUT
	SEARCH
	GET
	DELETEPREFIX
	TRYLOCK
	TRYUNLOCK
)

var (
	revisionKey = []byte("__SERVER_KEY_REVISION__")
	lockKey     = []byte("__LOCK_KEY__")
)

type WriteOptions struct {
	// 存储key时，此key的revision
	Revision uint64
}

func mergeWriteOptions(opts ...*WriteOptions) *WriteOptions {
	wo := &WriteOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		wo.Revision = opt.Revision
	}

	return wo
}

type RaftCommand interface {
	// 命令需要线性调用
	Linear() bool

	// 命令的类型
	GetType() CommandType

	// 线性调用的方法
	RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, session *client.Session) error

	// 本地调用的方法
	LocalInvoke(s *store.Store, opts ...*WriteOptions) error

	// 获取执行结果
	GetResp() []byte
}

func Encode(e interface{}) []byte {
	buf := bytes.NewBuffer(nil)

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	_ = encoder.Encode(e)
	return buf.Bytes()
}

func Decode(buf []byte, e interface{}) error {
	handle := codec.MsgpackHandle{}
	return codec.NewDecoder(bytes.NewReader(buf), &handle).Decode(e)
}

func EncodeCmd(cmd RaftCommand) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(cmd.GetType()))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(cmd)
	return buf.Bytes(), err
}

func DecodeCmd(data []byte) (RaftCommand, error) {
	var cmd RaftCommand
	switch CommandType(data[0]) {
	case DELETE:
		cmd = &DelCommand{}
	case PUT:
		cmd = &PutCommand{}
	case SEARCH:
		cmd = &SearchCommand{}
	case GET:
		cmd = &GetCommand{}
	case DELETEPREFIX:
		cmd = &DelPrefixCommand{}
	case TRYLOCK:
		cmd = &TryLockCommand{}
	case TRYUNLOCK:
		cmd = &TryUnlockCommand{}
	default:
		return nil, errors.New("can not find command type:" + strconv.Itoa(int(data[0])))
	}

	handle := codec.MsgpackHandle{}
	return cmd, codec.NewDecoder(bytes.NewReader(data[1:]), &handle).Decode(cmd)
}

func syncWrite(ctx context.Context, nh *dragonboat.NodeHost, session *client.Session, cmd RaftCommand) ([]byte, error) {
	b, err := EncodeCmd(cmd)
	if err != nil {
		return nil, err
	}

	result, err := nh.SyncPropose(ctx, session, b)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func syncRead(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, cmd RaftCommand) ([]byte, error) {
	b, err := EncodeCmd(cmd)
	if err != nil {
		return nil, err
	}

	result, err := nh.SyncRead(ctx, clusterId, b)
	if err != nil {
		return nil, err
	}

	return result.([]byte), nil
}

func buildRevisionKey(key []byte) []byte {
	return append(revisionKey, key...)
}
