package store

import "sync"

type DB interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
}

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStore() DB {
	return &Store{
		data: make(map[string][]byte),
	}
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return val, true
}
