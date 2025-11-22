package gossip

import "github.com/AyKrimino/kriminoDB/internal/store"

type MessageType string

const (
	Join         MessageType = "JOIN"
	JoinResponse MessageType = "JOIN_RESPONSE"
)

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

type JoinResponseMessage struct {
	Type     MessageType                `json:"type"`
	Peers    []string                   `json:"peers"`
	Snapshot map[string]store.DataValue `json:"snapshot"`
}

func NewJoinResponseMessage() *JoinResponseMessage {
	return &JoinResponseMessage{
		Type: JoinResponse,
		Peers: make([]string, 0, 20),
		Snapshot: map[string]store.DataValue{},
	}
}
