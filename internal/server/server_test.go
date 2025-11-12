package server

import (
	"testing"

	"github.com/AyKrimino/kriminoDB/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCommands(t *testing.T) {
	// Test: 
	conf := Config{
		Host: "localhost",
		Port: "3000",
	}

	st := store.NewStore()
	srv := NewServer(st, conf).(*server)

	// Test: Valid parse commands of get key syntax
	cmd, parts, err := srv.parseCommands("get key")
	require.Nil(t, err)
	require.NotNil(t, parts)
	require.NotZero(t, cmd)
	assert.Equal(t, "GET", cmd)
	assert.Equal(t, []string{"key"}, parts)

	// Test: Valid parse commands of set key value syntaxt
	cmd, parts, err = srv.parseCommands("set key value")
	require.Nil(t, err)
	require.NotNil(t, parts)
	require.NotZero(t, cmd)
	assert.Equal(t, "SET", cmd)
	assert.Equal(t, []string{"key", "value"}, parts)
}
