// Package store provides an in-memory key–value database
package store

import (
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
		version: time.Now().Unix(),
	}
}

// Store is an in-memory implementation of the DB interface.
type Store struct {
	mu   sync.RWMutex
	data map[string]dataValue
}

func NewStore() DB {
	return &Store{
		data: make(map[string]dataValue),
	}
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = *NewDataValue(value)
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
