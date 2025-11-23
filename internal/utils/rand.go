package utils

import (
	"math/rand"
	"time"
)

func GetRandomItems[T any](items []T, n int) []T {
	if n <= 0 {
		return nil
	}
	if n > len(items) {
		n = len(items)
	}

	shuffled := make([]T, len(items))
	copy(shuffled, items)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled[:n]
}
