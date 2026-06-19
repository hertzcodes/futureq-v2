package utils

import "time"

func CalculateBucket(executeAt int64, bucketSize time.Duration) uint64 {
	if executeAt <= 0 {
		return 0
	}

	bucketSizeMs := bucketSize.Milliseconds()
	if bucketSizeMs > 0 {
		k := (executeAt + bucketSizeMs - 1) / bucketSizeMs
		return uint64(k * bucketSizeMs)
	}

	return uint64(executeAt)
}