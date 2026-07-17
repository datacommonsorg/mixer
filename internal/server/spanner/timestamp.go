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

import "time"

const (
	timestampDuration = 1 * time.Minute
)

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type TimestampTicker struct {
	ticker *time.Ticker
}

// C implements the Ticker interface.
func (t *TimestampTicker) C() <-chan time.Time {
	return t.ticker.C
}

// Stop stops the underlying time.Ticker.
func (t *TimestampTicker) Stop() {
	t.ticker.Stop()
}

// NewTimestampTicker returns a new Ticker interface using the real implementation.
func NewTimestampTicker() Ticker {
	return &TimestampTicker{ticker: time.NewTicker(timestampDuration)}
}
