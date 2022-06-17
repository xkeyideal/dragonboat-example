package gossip

import (
	"bytes"
	"encoding/json"

	"github.com/xkeyideal/dragonboat-example/v3/tools"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist"
)

// messageType are the types of gossip messages will send along
// memberlist.
type messageType uint8

const (
	messageShardType messageType = iota
	messageMembershipType
	messagePushPullType
)

type TargetShardId struct {
	GrpcAddr string   `json:"grpcAddr"`
	ShardIds []uint64 `json:"shardIds"`
}

type RaftShardMessage struct {
	Revision int64 `json:"revision"`

	// 机器ID对应的MoveTo的GRPC地址
	// key: 当raft以nodehostid=true的方式起的时候是机器ID，以固定地址方式起是raftAddr
	Targets map[string]TargetShardId `json:"targets"`

	// 每个raft shard对应的机器ID|raftAddr
	Shards map[uint64][]string `json:"shards"`

	// 每个raft shard的initial members
	// key: shardId, key: replicaId, val: raftAddr或nodeHostID
	InitialMembers map[uint64]map[uint64]string `json:"initial_members"`
	Join           map[uint64]map[uint64]bool   `json:"join"`
}

func (rm *RaftShardMessage) String() string {
	b, _ := json.Marshal(rm)
	return string(b)
}

type MemberInfo struct {
	ShardId        uint64
	ConfigChangeId uint64
	Replicas       map[uint64]string
	Observers      map[uint64]string
	LeaderId       uint64
	LeaderValid    bool
}

func (mi *MemberInfo) String() string {
	b, _ := json.Marshal(mi)
	return string(b)
}

type RaftMembershipMessage struct {
	// key: shardId
	MemberInfos map[uint64]*MemberInfo
}

func (rm *RaftMembershipMessage) String() string {
	b, _ := json.Marshal(rm)
	return string(b)
}

type PushPullMessage struct {
	Shard      *RaftShardMessage
	Membership *RaftMembershipMessage
}

func (pp *PushPullMessage) String() string {
	b, _ := json.Marshal(pp)
	return string(b)
}

func decodeMessage(buf []byte, out interface{}) error {
	bbuf, err := tools.GZipDecode(buf)
	if err != nil {
		return err
	}

	var handle codec.MsgpackHandle
	return codec.NewDecoder(bytes.NewReader(bbuf), &handle).Decode(out)
}

func encodeMessage(t messageType, msg interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(msg)
	if err != nil {
		return nil, err
	}

	gbuf, err := tools.GZipEncode(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return append([]byte{uint8(t)}, gbuf...), nil
}

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func newBroadcast(msg []byte) *broadcast {
	return &broadcast{
		msg:    msg,
		notify: make(chan struct{}),
	}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}
