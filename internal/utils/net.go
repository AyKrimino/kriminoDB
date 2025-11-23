package utils

import (
	"strconv"
	"strings"
)

// IsPort checks if a port string is uint16 (unsigned int from 0 to 2¹⁶-1).
func IsPort(portStr string) bool {
	_, err := strconv.ParseUint(portStr, 10, 16)
	return err == nil
}

// IsIPAddress checks if str is following this format "X.X.X.X"
// where each X is between 0 and 2⁸-1.
func IsIPAddress(str string) bool {
	parts := strings.Split(str, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		_, err := strconv.ParseUint(part, 10, 8)
		if err != nil {
			return false
		}
	}
	return true
}
