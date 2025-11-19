package store

import (
	"log"
	"sync"
	"time"
)

type DB interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
}

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
