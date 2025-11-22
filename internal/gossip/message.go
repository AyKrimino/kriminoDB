package gossip

import "github.com/AyKrimino/kriminoDB/internal/store"

type MessageType string

const (
	Join         MessageType = "JOIN"
	JoinResponse MessageType = "JOIN_RESPONSE"
)

// JoinMessage represents a message of type JOIN.
// It is used when a new peer wants to join the cluster
// using a known bootstrap address.
type JoinMessage struct {
	Type   MessageType `json:"type"`
	Sender string      `json:"sender"`
}

func NewJoinMessage(sender string) *JoinMessage {
	return &JoinMessage{
		Type: Join,
		Sender: sender,
	}
}

// JoinResponseMessage represents a message of type JOIN_RESPONSE.
// It is used when the bootstrap peer got a messgae of type
// JOIN to tell it what peers it has and a Snapshot of its store.
type JoinResponseMessage struct {
	Type     MessageType                `json:"type"`
	Peers    []string                   `json:"peers"`
	Snapshot map[string]store.DataValue `json:"snapshot"`
}

func NewJoinResponseMessage(peers []string) *JoinResponseMessage {
	return &JoinResponseMessage{
		Type: JoinResponse,
		Peers: peers,
		Snapshot: map[string]store.DataValue{},
	}
}
