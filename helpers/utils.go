package helpers

import (
	"strconv"
	"strings"
	"time"
)

const (
	millisPerSecond     = int64(time.Second / time.Millisecond)
	nanosPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

// Add a right pad
func RightPad(s string, padStr string, pLen int) string {
	if pLen > 0 {
			return s + strings.Repeat(padStr, pLen)
	}
	return s
}

// Convert milliseconds to Unix time
func MsToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(msInt/millisPerSecond,
		(msInt%millisPerSecond)*nanosPerMillisecond), nil
}
