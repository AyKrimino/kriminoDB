package gossip

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strings"

	"github.com/AyKrimino/kriminoDB/internal/store"
	"github.com/AyKrimino/kriminoDB/internal/utils"
)

type MessageType string

const (
	JoinType         MessageType = "JOIN"
	JoinResponseType MessageType = "JOIN_RESPONSE"
	UpdateType       MessageType = "UPDATE"
	GossipType       MessageType = "GOSSIP"
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
		Type:   JoinType,
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
		Type:     JoinResponseType,
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
		Type:      UpdateType,
		Key:       key,
		DataValue: dv,
	}
}

type MessageHeader struct {
	Type MessageType `json:"type"`
}

type GossipMessage struct {
	Type      MessageType                `json:"type"`
	MessageID []byte                     `json:"message_id"`
	Updates   map[string]store.DataValue `json:"updates"`
	Peers     []string
}

func NewGossipMessage(updates map[string]store.DataValue, peers []string) *GossipMessage {
	gm := &GossipMessage{
		Type:    GossipType,
		Updates: updates,
		Peers:   peers,
	}

	gm.MessageID = gm.computeMessageID()

	return gm
}

func (gm GossipMessage) computeMessageID() []byte {
	h := sha256.New()

	keys := make([]string, 0, len(gm.Updates))
	for k := range gm.Updates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		h.Write([]byte(k))

		versionBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(versionBuf, uint64(gm.Updates[k].Version))
		h.Write(versionBuf)
	}

	sortedPeers := make([]string, len(gm.Peers))
	copy(sortedPeers, gm.Peers)
	sort.Strings(sortedPeers)

	for _, p := range sortedPeers {
		h.Write([]byte(p))
	}

	return h.Sum(nil)
}
