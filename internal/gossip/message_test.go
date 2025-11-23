package gossip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinResponseMessage_Validate(t *testing.T) {
	// Valid: localhost and IPv4 with valid ports
	msg := JoinResponseMessage{
		Peers: []string{
			"localhost:8080",
			"192.168.1.10:3000",
		},
	}
	require.True(t, msg.Validate())

	// Invalid: hostname other than localhost
	msg = JoinResponseMessage{
		Peers: []string{"myhost:9000"},
	}
	require.False(t, msg.Validate())

	// Invalid: ports bigger than uint16
	msg = JoinResponseMessage{
		Peers: []string{"localhost:999999"},
	}
	require.False(t, msg.Validate())
}
