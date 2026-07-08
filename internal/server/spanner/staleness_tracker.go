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
	"fmt"
	"log/slog"
	"time"
)

type logEvent struct {
	level   slog.Level
	message string
	args    []interface{}
}

type stalenessTracker struct {
	noChangeThreshold time.Duration
	failureThreshold  time.Duration

	lastSuccessTime        time.Time
	lastChangeTime         time.Time
	lastLoggedNoChangeTime time.Time
	lastLoggedFailureTime  time.Time
}

func newStalenessTracker(noChangeThreshold, failureThreshold time.Duration) *stalenessTracker {
	return &stalenessTracker{
		noChangeThreshold: noChangeThreshold,
		failureThreshold:  failureThreshold,
	}
}

func formatTimestamp(val int64) string {
	if val == 0 {
		return "none (strong read fallback)"
	}
	return time.Unix(0, val).UTC().String()
}

func (t *stalenessTracker) RecordSuccess(now time.Time, prevVal, newVal int64) *logEvent {
	t.lastSuccessTime = now
	t.lastLoggedFailureTime = time.Time{} // Reset failure log time

	if prevVal == 0 {
		t.lastChangeTime = now
		t.lastLoggedNoChangeTime = now
		return &logEvent{
			level:   slog.LevelInfo,
			message: "Spanner staleness timestamp initialized",
			args:    []interface{}{"timestamp", formatTimestamp(newVal)},
		}
	}

	if prevVal != newVal {
		t.lastChangeTime = now
		t.lastLoggedNoChangeTime = now
		return &logEvent{
			level:   slog.LevelInfo,
			message: "Spanner staleness timestamp updated",
			args:    []interface{}{"old", formatTimestamp(prevVal), "new", formatTimestamp(newVal)},
		}
	}

	// Value hasn't changed.
	if t.lastChangeTime.IsZero() {
		t.lastChangeTime = now
		t.lastLoggedNoChangeTime = now
	}

	if now.Sub(t.lastChangeTime) >= t.noChangeThreshold && now.Sub(t.lastLoggedNoChangeTime) >= t.noChangeThreshold {
		t.lastLoggedNoChangeTime = now
		return &logEvent{
			level:   slog.LevelInfo,
			message: fmt.Sprintf("Spanner staleness timestamp has not changed in the last %s", t.noChangeThreshold),
			args: []interface{}{
				"timestamp", formatTimestamp(newVal),
				"durationSinceLastChange", now.Sub(t.lastChangeTime).Round(time.Second).String(),
			},
		}
	}

	return nil
}

func (t *stalenessTracker) RecordFailure(now time.Time, err error) *logEvent {
	if t.lastSuccessTime.IsZero() {
		// If we never succeeded, we don't log the persistent failure warning
		// since we already log the direct error on every tick.
		return nil
	}

	if now.Sub(t.lastSuccessTime) >= t.failureThreshold {
		if t.lastLoggedFailureTime.IsZero() || now.Sub(t.lastLoggedFailureTime) >= t.failureThreshold {
			t.lastLoggedFailureTime = now
			return &logEvent{
				level:   slog.LevelWarn,
				message: fmt.Sprintf("Spanner staleness timestamp poller has failed to run successfully for %s", t.failureThreshold),
				args: []interface{}{
					"lastSuccessTime", t.lastSuccessTime.UTC().String(),
					"error", err,
				},
			}
		}
	}

	return nil
}
