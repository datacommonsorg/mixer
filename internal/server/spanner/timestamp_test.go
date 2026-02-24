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
	"testing"
	"time"
)

type MockTicker struct {
	channel chan time.Time
}

func NewMockTicker() *MockTicker {
	return &MockTicker{
		channel: make(chan time.Time, 1),
	}
}

func (mt *MockTicker) C() <-chan time.Time {
	return mt.channel
}

func (mt *MockTicker) Stop() {
	// No-op for mock
}

func (mt *MockTicker) Tick() {
	select {
	case mt.channel <- time.Now():
	default:
		// Prevent blocking if the goroutine isn't listening
	}
}

func TestTimestampUpdated(t *testing.T) {
	var updateCount int
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	updateDone := make(chan bool, 1)

	sc := &spannerDatabaseClient{
		useStaleReads: true,
		ticker:        mockTicker,
		stopCh:        make(chan struct{}),
	}
	// Store intial timestamp.
	sc.timestamp.Store(startTime.UnixNano())

	// Mock successful update.
	sc.updateTimestamp = func(context.Context) error {
		updateCount++
		nextTimestamp := startTime.Add(1 * time.Hour).UnixNano()
		sc.timestamp.Store(nextTimestamp)
		updateDone <- true
		return nil
	}

	sc.Start()
	mockTicker.Tick()

	<-updateDone
	sc.Close()

	if updateCount != 1 {
		t.Fatalf("Expected updateTimestamp to be called 1 time, got %d", updateCount)
	}
	timestamp, err := sc.getStalenessTimestamp()
	if err != nil {
		t.Fatalf("Error getting staleness timestamp")
	}

	// Expect timestamp to be updated.
	expectedTimestamp := startTime.Add(1 * time.Hour)
	if timestamp != expectedTimestamp {
		t.Fatalf("Expected timestamp to be %v, but got %v", expectedTimestamp, timestamp)
	}
}

func TestTimestampUpdateFailure(t *testing.T) {
	var count int
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	updateDone := make(chan bool, 1)

	sc := &spannerDatabaseClient{
		useStaleReads: true,
		ticker:        mockTicker,
		stopCh:        make(chan struct{}),
	}
	// Store initial timestamp
	sc.timestamp.Store(startTime.UnixNano())

	// Mock failed update.
	sc.updateTimestamp = func(context.Context) error {
		count++
		updateDone <- true
		return fmt.Errorf("test spanner failure")
	}

	sc.Start()
	mockTicker.Tick()

	<-updateDone
	sc.Close()

	if count != 1 {
		t.Fatalf("Expected updateTimestamp to be called 1 time, got %d", count)
	}
	timestamp, err := sc.getStalenessTimestamp()
	if err != nil {
		t.Fatalf("Error getting staleness timestamp")
	}

	// Expect timestamp to not have been updated.
	expectedTimestamp := startTime
	if timestamp != expectedTimestamp {
		t.Fatalf("Expected timestamp to be %v, but got %v", expectedTimestamp, timestamp)
	}
}
