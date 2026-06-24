package storage

import "time"

type (
	Bucket   = time.Time
	Topic    = string
	Messages = [][]byte
)

// This is unused for now.
type BucketCache struct {
	storage map[Bucket]map[Topic]Messages
}

func (bc *BucketCache) CacheMessage(bucket Bucket, topic Topic, message []byte) {
	bc.storage[bucket][topic] = append(bc.storage[bucket][topic], message)
}

func (bc *BucketCache) GetExpired(bucket Bucket, validTopics map[Topic]struct{}) Messages {
	var result [][]byte

	for b, topics := range bc.storage {
		if bucket.Sub(b) < 0 {
			for topic, msgs := range topics {
				if _, ok := validTopics[topic]; ok {
					result = append(result, msgs...)
				}
			}
		}
	}

	return result
}
