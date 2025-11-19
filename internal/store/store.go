// Package store provides an in-memory key–value database
package store

import (
	"maps"
	"sync"
	"time"
)

// DB defines the basic operations of a key–value store.
type DB interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
}

// dataValue stores the raw byte value and a version timestamp.
type dataValue struct {
	value   []byte
	version int64
}

func NewDataValue(value []byte) *dataValue {
	return &dataValue{
		value: value,
		version: time.Now().UnixNano(),
	}
}

// Store is an in-memory implementation of the DB interface.
type Store struct {
	mu   sync.RWMutex
	data map[string]dataValue
	pendingUpdates map[string]dataValue // TODO: add capacity
}

func NewStore() DB {
	return &Store{
		data: make(map[string]dataValue),
		pendingUpdates: make(map[string]dataValue),
	}
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dv := *NewDataValue(value)
	s.data[key] = dv
	s.pendingUpdates[key] = dv
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return val.value, true
}

// GetPendingUpdates returns a copy of pending updates for gossip operations
func (s* Store) GetPendingUpdates() map[string]dataValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pending := make(map[string]dataValue)
	maps.Copy(pending, s.pendingUpdates)
	return pending
}
