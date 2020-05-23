package utils

import (
	"math/rand"
	"time"
)

func Retry(sleep time.Duration, fn func() error) error {
	if err := fn(); err != nil {
		if s, ok := err.(stop); ok {
			// Return the original error for later checking
			return s.error
		}
		jitter := time.Duration(rand.Int63n(int64(sleep)))
		sleep = sleep + jitter/2
		time.Sleep(sleep)
		return Retry(2*sleep, fn)
	}
	return nil
}

type stop struct {
	error
}
