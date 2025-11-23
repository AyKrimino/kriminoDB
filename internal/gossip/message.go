package gossip

import (
	"strings"

	"github.com/AyKrimino/kriminoDB/internal/store"
	"github.com/AyKrimino/kriminoDB/internal/utils"
)

type MessageType string

const (
	Join         MessageType = "JOIN"
	JoinResponse MessageType = "JOIN_RESPONSE"
	Update       MessageType = "UPDATE"
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
		Type:   Join,
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
		Type:     JoinResponse,
		Peers:    peers,
		Snapshot: map[string]store.DataValue{},
	}
}

// Validate check if all strings in Peers are following this
// format "host:port".
func (jr JoinResponseMessage) Validate() bool {
	for _, p := range jr.Peers {
		parts := strings.Split(p, ":")
		if len(parts) != 2 {
			return false
		}
		host, port := parts[0], parts[1]

		if utils.IsAlpha(host) {
			if host != "localhost" {
				return false
			}
		} else if !utils.IsIPAddress(host) {
			return false
		}

		if !utils.IsPort(port) {
			return false
		}
	}
	return true
}

type UpdateMessage struct {
	Type      MessageType     `json:"type"`
	Key       string          `json:"key"`
	DataValue store.DataValue `json:"data_value"`
}

func NewUpdateMessge(key string, dv store.DataValue) *UpdateMessage {
	return &UpdateMessage{
		Type: Update,
		Key: key,
		DataValue: dv,
	}
}

type MessageHeader struct {
	Type MessageType `json:"type"`
}
