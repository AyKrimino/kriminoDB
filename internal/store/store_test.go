package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreSetGet(t *testing.T) {
	// Test: key exist
	s := NewStore()
	s.Set("key", []byte("value"))
	v, exists := s.Get("key")
	require.NotNil(t, v)
	require.True(t, exists)
	assert.Equal(t, []byte("value"), v)

	// Test: key does not exist
	s = NewStore()
	v, exists = s.Get("notexist")
	require.Nil(t, v)
	require.False(t, exists)
}

func TestStoreGetPendingUpdates(t *testing.T) {
	s := NewStore().(*Store)

	// SET test hello
	s.Set("test", []byte("hello"))
	pending := s.GetPendingUpdates()
	require.NotZero(t, pending)

	dv, ok := pending["test"]
	require.True(t, ok)
	require.NotNil(t, dv)

	value, version := dv.value, dv.version
	assert.Equal(t, []byte("hello"), value)

	// SET test world
	s.Set("test", []byte("world"))
	pending = s.GetPendingUpdates()
	require.NotZero(t, pending)

	dv, ok = pending["test"]
	require.True(t, ok)
	require.NotNil(t, dv)

	value, newVersion := dv.value, dv.version
	assert.Equal(t, []byte("world"), value)

	assert.Greater(t, newVersion, version)
}
