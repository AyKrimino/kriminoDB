package gossip

import "github.com/AyKrimino/kriminoDB/internal/store"

type MessageType string

const (
	Join         MessageType = "JOIN"
	JoinResponse MessageType = "JOIN_RESPONSE"
)

type JoinMessage struct {
	Type   MessageType
	Sender string
}

type JoinResponseMessage struct {
	Type  MessageType
	Peers []string
	Snapshot map[string]store.DataValue
}
