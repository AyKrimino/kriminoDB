// Package store provides an in-memory key–value database
package store

import (
	"fmt"
	"maps"
	"sync"
	"time"
)

// DB defines the basic operations of a key–value store.
type DB interface {
	Get(key string) (DataValue, bool)
	Set(key string, value []byte)
	Update(key string, dataValue DataValue) error
}

// DataValue stores the raw byte value and a version timestamp.
type DataValue struct {
	Value   []byte `json:"value"`
	Version int64  `json:"version"`
}

func NewDataValue(value []byte) *DataValue {
	return &DataValue{
		Value:   value,
		Version: time.Now().UnixNano(),
	}
}

// Store is an in-memory implementation of the DB interface.
type Store struct {
	replicator Replicator

	mu             sync.RWMutex
	data           map[string]DataValue
	pendingUpdates map[string]DataValue // TODO: add capacity
}

func NewStore(replicator Replicator) DB {
	return &Store{
		replicator:     replicator,
		data:           make(map[string]DataValue),
		pendingUpdates: make(map[string]DataValue),
	}
}

func (s *Store) SetReplicator(r Replicator) {
	s.replicator = r
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dv := *NewDataValue(value)
	s.data[key] = dv
	s.pendingUpdates[key] = dv

	go func() {
		if s.replicator != nil {
			s.replicator.Replicate(key, dv)
		}
	}()
}

func (s *Store) Update(key string, dataValue DataValue) error {
	oldDataValue, exists := s.Get(key)
	if !exists {
		return fmt.Errorf("key %s does not exist", key)
	}

	s.setExistingKey(key, dataValue, oldDataValue)
	return nil
}

func (s *Store) setExistingKey(key string, newDataValue DataValue, oldDataValue DataValue) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if newDataValue.Version > oldDataValue.Version {
		s.data[key] = newDataValue
		s.pendingUpdates[key] = newDataValue
	}
}

func (s *Store) Get(key string) (DataValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dv, ok := s.data[key]
	if !ok {
		return DataValue{}, false
	}
	return dv, true
}

// GetPendingUpdates returns a copy of pending updates for gossip operations
func (s *Store) GetPendingUpdates() map[string]DataValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pending := make(map[string]DataValue)
	maps.Copy(pending, s.pendingUpdates)
	return pending
}
