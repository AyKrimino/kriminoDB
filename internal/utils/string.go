// Package utils provides helper functions for string and rune validation.
package utils

func IsLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')
}

func IsAlpha(str string) bool {
	for _, r := range str {
		if !IsLetter(r) {
			return false
		}
	}
	return str != ""
}
