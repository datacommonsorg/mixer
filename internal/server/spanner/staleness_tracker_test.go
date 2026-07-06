// Copyright 2026 Google LLC
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
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestStalenessTracker(t *testing.T) {
	noChangeThresh := 10 * time.Minute
	failureThresh := 10 * time.Minute
	tracker := newStalenessTracker(noChangeThresh, failureThresh)

	baseTime := time.Date(2026, time.July, 6, 12, 0, 0, 0, time.UTC)
	initVal := int64(123456789)

	// 1. First success should initialize
	event := tracker.RecordSuccess(baseTime, 0, initVal)
	if event == nil {
		t.Fatal("expected initialization event, got nil")
	}
	if event.level != slog.LevelInfo || event.message != "Spanner staleness timestamp initialized" {
		t.Errorf("unexpected init event: %+v", event)
	}

	// 2. Poll succeeding with same value at baseTime + 1m should return nil (no change, below threshold)
	event = tracker.RecordSuccess(baseTime.Add(time.Minute), initVal, initVal)
	if event != nil {
		t.Errorf("expected nil event for same value within threshold, got %+v", event)
	}

	// 3. Poll succeeding with same value at baseTime + 11m should trigger "no change" event
	event = tracker.RecordSuccess(baseTime.Add(11*time.Minute), initVal, initVal)
	if event == nil {
		t.Fatal("expected no-change event after 11m, got nil")
	}
	if event.level != slog.LevelInfo || event.message != "Spanner staleness timestamp has not changed in the last 10m0s" {
		t.Errorf("unexpected no-change event: %+v", event)
	}

	// 4. Poll succeeding with same value at baseTime + 12m should return nil (no-change threshold already logged)
	event = tracker.RecordSuccess(baseTime.Add(12*time.Minute), initVal, initVal)
	if event != nil {
		t.Errorf("expected nil event at 12m, got %+v", event)
	}

	// 5. Poll succeeding with same value at baseTime + 22m should trigger "no change" event again (since 11 minutes have passed since last log)
	event = tracker.RecordSuccess(baseTime.Add(22*time.Minute), initVal, initVal)
	if event == nil {
		t.Fatal("expected second no-change event at 22m, got nil")
	}

	// 6. Value changes at baseTime + 25m
	newVal := int64(987654321)
	event = tracker.RecordSuccess(baseTime.Add(25*time.Minute), initVal, newVal)
	if event == nil {
		t.Fatal("expected update event, got nil")
	}
	if event.level != slog.LevelInfo || event.message != "Spanner staleness timestamp updated" {
		t.Errorf("unexpected update event: %+v", event)
	}

	// 7. Poller fails at baseTime + 26m (1 minute after last success)
	testErr := errors.New("connection reset")
	event = tracker.RecordFailure(baseTime.Add(26*time.Minute), testErr)
	if event != nil {
		t.Errorf("expected nil event for single failure, got %+v", event)
	}

	// 8. Poller fails continuously, at baseTime + 36m (11 minutes after last success)
	event = tracker.RecordFailure(baseTime.Add(36*time.Minute), testErr)
	if event == nil {
		t.Fatal("expected failure event after 11m of failures, got nil")
	}
	if event.level != slog.LevelWarn || event.message != "Spanner staleness timestamp poller has failed to run successfully for 10m0s" {
		t.Errorf("unexpected failure warning event: %+v", event)
	}

	// 9. Failure at baseTime + 37m should be nil (logged within threshold)
	event = tracker.RecordFailure(baseTime.Add(37*time.Minute), testErr)
	if event != nil {
		t.Errorf("expected nil event at 37m, got %+v", event)
	}

	// 10. Success at baseTime + 38m resets the failure state
	event = tracker.RecordSuccess(baseTime.Add(38*time.Minute), newVal, newVal)
	// We might get nil if no-change is not reached, but failure should be reset
	// Let's verify failure log is reset by triggering a failure at 39m (nil since it's only 1m since last success)
	event = tracker.RecordFailure(baseTime.Add(39*time.Minute), testErr)
	if event != nil {
		t.Errorf("expected nil event at 39m, got %+v", event)
	}
}
