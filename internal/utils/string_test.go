package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsLetter(t *testing.T) {
	// Test: uppercase letters
	for r := 'A'; r <= 'Z'; r++ {
		assert.True(t, IsLetter(r), "Expected %c to be a letter", r)
	}

	// Test: lowercase letters
	for r := 'a'; r <= 'z'; r++ {
		assert.True(t, IsLetter(r), "Expected %c to be a letter", r)
	}

	// Test: non-letter runes
	require.False(t, IsLetter('0'))
	require.False(t, IsLetter(' '))
	require.False(t, IsLetter('-'))
	require.False(t, IsLetter('!'))
	require.False(t, IsLetter(rune(0)))
}

func TestIsAlpha(t *testing.T) {
	// Test: valid alphabetic strings
	require.True(t, IsAlpha("hello"))
	require.True(t, IsAlpha("Hello"))
	require.True(t, IsAlpha("a"))
	require.True(t, IsAlpha("HeLLo"))

	// Test: invalid alphabetic strings
	require.False(t, IsAlpha(""))
	require.False(t, IsAlpha("hello123"))
	require.False(t, IsAlpha("hello-world"))
	require.False(t, IsAlpha("hello world"))
	require.False(t, IsAlpha("123"))
}
