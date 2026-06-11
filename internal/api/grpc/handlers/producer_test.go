package handlers

import (
	"testing"
	"time"
)

func TestCalculateBucket(t *testing.T) {
	tests := []struct {
		name       string
		executeAt  int64
		bucketSize time.Duration
		expected   uint64
	}{
		{
			name:       "exact multiple of 1s",
			executeAt:  17000,
			bucketSize: 1 * time.Second,
			expected:   17000,
		},
		{
			name:       "slightly over multiple of 1s",
			executeAt:  17001,
			bucketSize: 1 * time.Second,
			expected:   18000,
		},
		{
			name:       "slightly under next multiple of 1s",
			executeAt:  17999,
			bucketSize: 1 * time.Second,
			expected:   18000,
		},
		{
			name:       "exactly 0",
			executeAt:  0,
			bucketSize: 1 * time.Second,
			expected:   0,
		},
		{
			name:       "negative value",
			executeAt:  -100,
			bucketSize: 1 * time.Second,
			expected:   0,
		},
		{
			name:       "bucket size is 0",
			executeAt:  17300,
			bucketSize: 0,
			expected:   17300,
		},
		{
			name:       "bucket size is 500ms, exact multiple",
			executeAt:  1500,
			bucketSize: 500 * time.Millisecond,
			expected:   1500,
		},
		{
			name:       "bucket size is 500ms, round up",
			executeAt:  1501,
			bucketSize: 500 * time.Millisecond,
			expected:   2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateBucket(tt.executeAt, tt.bucketSize)
			if got != tt.expected {
				t.Errorf("calculateBucket(%d, %v) = %d; want %d", tt.executeAt, tt.bucketSize, got, tt.expected)
			}
		})
	}
}
