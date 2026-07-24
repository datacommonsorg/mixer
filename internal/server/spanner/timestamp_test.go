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

	"cloud.google.com/go/spanner"
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
		ticker: mockTicker,
		stopCh: make(chan struct{}),
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
		ticker: mockTicker,
		stopCh: make(chan struct{}),
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

// Test: Timestamp reset on null or completed run.
// Situation: The client previously held an active ingestion timestamp when the database transitioned to returning null on run completion.
// Expectation: The helper zeros out the timestamp in production code so getStalenessTimestamp returns an error to trigger exact staleness reads.
func TestTimestampResetOnNull(t *testing.T) {
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	updateDone := make(chan bool, 1)

	sc := &spannerDatabaseClient{
		ticker:  mockTicker,
		stopCh:  make(chan struct{}),
		tracker: newStalenessTracker(noChangeLogThreshold, failureLogThreshold),
	}
	sc.timestamp.Store(startTime.UnixNano())

	// Delegate directly to production logic via processStalenessTimestamp without mocking internal mutations.
	sc.updateTimestamp = func(ctx context.Context) error {
		err := sc.processStalenessTimestamp(ctx, spanner.NullTime{Valid: false})
		updateDone <- true
		return err
	}

	sc.Start()
	mockTicker.Tick()

	<-updateDone
	sc.Close()

	if val := sc.timestamp.Load(); val != 0 {
		t.Fatalf("Expected sc.timestamp to be reset to 0, but got %d", val)
	}

	// Expect getStalenessTimestamp to return an error when zero so executeQuery invokes exact staleness fallback reads.
	if _, err := sc.getStalenessTimestamp(); err == nil {
		t.Fatalf("Expected error when staleness timestamp is zero, but got nil")
	}
}

func TestSetOnIngestionUpdate_TriggersOnTimestampChange(t *testing.T) {
	t.Parallel()
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	updatedTime := startTime.Add(1 * time.Hour)

	sc := &spannerDatabaseClient{
		ticker: mockTicker,
		stopCh: make(chan struct{}),
	}
	sc.timestamp.Store(startTime.UnixNano())
	sc.dbInitialized.Store(true)

	callbackCalled := make(chan bool, 1)
	sc.SetOnIngestionUpdate(func(ctx context.Context) {
		callbackCalled <- true
	})

	err := sc.processStalenessTimestamp(context.Background(), spanner.NullTime{Valid: true, Time: updatedTime})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-callbackCalled:
		// Success: callback was reactively invoked.
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for SetOnIngestionUpdate callback to be invoked")
	}
}

func TestSetOnIngestionUpdate_UnchangedTimestampNoTrigger(t *testing.T) {
	t.Parallel()
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)

	sc := &spannerDatabaseClient{
		ticker: mockTicker,
		stopCh: make(chan struct{}),
	}
	sc.timestamp.Store(startTime.UnixNano())
	sc.dbInitialized.Store(true)

	callbackCalled := make(chan bool, 1)
	sc.SetOnIngestionUpdate(func(ctx context.Context) {
		callbackCalled <- true
	})

	err := sc.processStalenessTimestamp(context.Background(), spanner.NullTime{Valid: true, Time: startTime})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-callbackCalled:
		t.Fatalf("Callback should NOT have been invoked when timestamp did not change")
	case <-time.After(100 * time.Millisecond):
		// Success: callback was NOT invoked.
	}
}

func TestSetOnIngestionUpdate_DrainsSubsequentUpdate(t *testing.T) {
	t.Parallel()
	mockTicker := NewMockTicker()
	startTime := time.Date(2025, time.January, 1, 10, 0, 0, 0, time.UTC)
	updatedTime1 := startTime.Add(1 * time.Hour)
	updatedTime2 := startTime.Add(2 * time.Hour)

	sc := &spannerDatabaseClient{
		ticker: mockTicker,
		stopCh: make(chan struct{}),
	}
	sc.timestamp.Store(startTime.UnixNano())
	sc.dbInitialized.Store(true)

	firstHookStarted := make(chan struct{})
	allowFirstHookToFinish := make(chan struct{})
	hookCallCount := make(chan int, 10)

	var callNum int
	sc.SetOnIngestionUpdate(func(ctx context.Context) {
		callNum++
		currentCall := callNum
		if currentCall == 1 {
			close(firstHookStarted)
			<-allowFirstHookToFinish
		}
		hookCallCount <- currentCall
	})

	// First timestamp update triggers first execution of hook
	err := sc.processStalenessTimestamp(context.Background(), spanner.NullTime{Valid: true, Time: updatedTime1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait until hook has entered execution
	<-firstHookStarted

	// While hook 1 is still executing, send a second timestamp update
	err = sc.processStalenessTimestamp(context.Background(), spanner.NullTime{Valid: true, Time: updatedTime2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now allow hook 1 to finish
	close(allowFirstHookToFinish)

	// Verify hook was executed twice (once for initial trigger, once for drained update)
	select {
	case c := <-hookCallCount:
		if c != 1 {
			t.Fatalf("Expected first hook call to be 1, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for first hook call")
	}

	select {
	case c := <-hookCallCount:
		if c != 2 {
			t.Fatalf("Expected second hook call to be 2, got %d", c)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for second drained hook call")
	}
}


