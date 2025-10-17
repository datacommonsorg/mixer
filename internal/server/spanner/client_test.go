// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCacheHit(t *testing.T) {
	var fetchCount int
	mockTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	stableTime := mockTime.Add(-1 * time.Minute)

	sc := &SpannerClient{clock: func() time.Time { return mockTime }}
	sc.timestampFetcher = func(ctx context.Context) (*time.Time, error) {
		fetchCount++
		return &stableTime, nil
	}
	// Initialization will populate cache
	sc.getCompletionTimestamp(context.Background())
	if fetchCount != 1 {
		t.Fatalf("Setup failed, expected 1 fetch, got %d", fetchCount)
	}

	// This call is immediately after initialization, within the 5-second duration.
	_, err := sc.getCompletionTimestamp(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if fetchCount != 1 {
		t.Errorf("Expected timestamp fetch count to remain 1, got %d", fetchCount)
	}
}

func TestCacheExpiration(t *testing.T) {
	var fetchCount int
	mockTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	stableTime := mockTime.Add(-1 * time.Minute)

	sc := &SpannerClient{
		cacheExpiry: mockTime.Add(CACHE_DURATION),
		clock:       func() time.Time { return mockTime },
	}
	sc.timestampFetcher = func(ctx context.Context) (*time.Time, error) {
		fetchCount++
		return &stableTime, nil
	}
	// Initialization will populate cache
	sc.getCompletionTimestamp(context.Background())
	if fetchCount != 1 {
		t.Fatalf("Setup failed, expected 1 fetch, got %d", fetchCount)
	}

	// Advance time past expiration
	mockTime = mockTime.Add(6 * time.Second)
	t.Run("Cache miss", func(t *testing.T) {
		_, err := sc.getCompletionTimestamp(context.Background())
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if fetchCount != 2 {
			t.Errorf("Expected timestamp fetch count to increase to 2, got %d", fetchCount)
		}
		expectedExpiry := mockTime.Add(CACHE_DURATION)
		if sc.cacheExpiry.Sub(expectedExpiry) > time.Millisecond {
			t.Errorf("Cache expiry was not correctly updated after refetch.")
		}
	})
}

func TestGetStalenessTimestampBound(t *testing.T) {
	mockTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	stableTime := mockTime.Add(-5 * time.Minute) // Stable time is 5 minutes ago

	sc := &SpannerClient{}
	sc.timestampFetcher = func(ctx context.Context) (*time.Time, error) {
		return &stableTime, nil
	}

	timestamp, err := sc.getStalenessTimestampBound(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if timestamp == nil {
		t.Fatal("Expected a non-nil TimestampBound")
	}

	// Approximate check relying on String() representation of ReadTimestamp.
	expectedString := fmt.Sprintf("ReadTimestamp(%s)", stableTime.Format(time.RFC3339Nano))
	actualString := (*timestamp).String()
	if !strings.Contains(actualString, stableTime.Format("2006-01-02")) {
		t.Errorf("Expected ReadTimestamp containing %v, got %s", expectedString, actualString)
	}
}
