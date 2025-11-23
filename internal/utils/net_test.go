package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsPort(t *testing.T) {
	// Valid port
	require.True(t, IsPort("8080"))

	// Invalid: out of uint16 range
	require.False(t, IsPort("70000"))
}

func TestIsIPAddress(t *testing.T) {
	// Valid IPv4
	require.True(t, IsIPAddress("192.168.1.1"))

	// Invalid: too many octets
	require.False(t, IsIPAddress("192.168.1.1.1"))
}
