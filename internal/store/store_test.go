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
