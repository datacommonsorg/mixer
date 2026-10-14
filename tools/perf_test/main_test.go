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

package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

func TestValidateInputsBulkVariableGroupInfoMethods(t *testing.T) {
	for _, tc := range []struct {
		name                string
		method              string
		nodes               string
		constrainedEntities string
		numEntities         int
		wantErr             bool
	}{
		{
			name:    "stat var group node accepts nodes",
			method:  "GetStatVarGroupNode",
			nodes:   "dc/g/Agriculture",
			wantErr: false,
		},
		{
			name:    "stat var group node rejects missing nodes",
			method:  "GetStatVarGroupNode",
			wantErr: true,
		},
		{
			name:                "filtered stat var group node accepts constraints",
			method:              "GetFilteredStatVarGroupNode",
			nodes:               "dc/g/Agriculture",
			constrainedEntities: "country/USA,country/IND",
			numEntities:         2,
			wantErr:             false,
		},
		{
			name:                "filtered topic accepts source constraint",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "dc/s/WorldBank",
			wantErr:             false,
		},
		{
			name:                "filtered topic rejects missing nodes",
			method:              "GetFilteredTopic",
			constrainedEntities: "country/USA",
			wantErr:             true,
		},
		{
			name:    "filtered topic rejects missing constraints",
			method:  "GetFilteredTopic",
			nodes:   "dc/topic/Demographics",
			wantErr: true,
		},
		{
			name:                "filtered topic rejects empty comma constraints",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: " , ",
			wantErr:             true,
		},
		{
			name:                "filtered topic rejects negative entity threshold",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "country/USA",
			numEntities:         -1,
			wantErr:             true,
		},
		{
			name:                "filtered topic rejects multiple import constraints",
			method:              "GetFilteredTopic",
			nodes:               "dc/topic/Demographics",
			constrainedEntities: "dc/s/WorldBank,dc/d/SomeDataset",
			wantErr:             true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInputs(tc.method, "", "", "", "", tc.nodes, tc.constrainedEntities, tc.numEntities)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateInputs() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseConstrainedEntities(t *testing.T) {
	places, constrainedImport, err := parseConstrainedEntities("country/USA, dc/s/WorldBank, country/IND")
	if err != nil {
		t.Fatalf("parseConstrainedEntities() returned error: %v", err)
	}
	if want := []string{"country/USA", "country/IND"}; !slices.Equal(places, want) {
		t.Fatalf("places = %v, want %v", places, want)
	}
	if want := "dc/s/WorldBank"; constrainedImport != want {
		t.Fatalf("constrainedImport = %q, want %q", constrainedImport, want)
	}
}

func TestParseConstrainedEntitiesRejectsMultipleImports(t *testing.T) {
	_, _, err := parseConstrainedEntities("dc/s/WorldBank,dc/d/Dataset")
	if err == nil {
		t.Fatal("parseConstrainedEntities() returned nil error, want error")
	}
}

func TestSummarizeDurations(t *testing.T) {
	for _, tc := range []struct {
		name      string
		durations []time.Duration
		want      latencySummary
	}{
		{
			name: "empty",
		},
		{
			name:      "odd count",
			durations: []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond},
			want:      latencySummary{runs: 3, min: 10 * time.Millisecond, median: 20 * time.Millisecond, mean: 20 * time.Millisecond, max: 30 * time.Millisecond},
		},
		{
			name:      "even count",
			durations: []time.Duration{40 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
			want:      latencySummary{runs: 4, min: 10 * time.Millisecond, median: 25 * time.Millisecond, mean: 25 * time.Millisecond, max: 40 * time.Millisecond},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := summarizeDurations(tc.durations); got != tc.want {
				t.Fatalf("summarizeDurations() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRunTimed(t *testing.T) {
	calls := 0
	durations, err := runTimed(context.Background(), 3, true, func(ctx context.Context) error {
		calls++
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok || len(md.Get(util.XLogSQL)) == 0 {
			t.Errorf("call %d: missing %s header", calls, util.XLogSQL)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runTimed() error = %v", err)
	}
	if calls != 3 || len(durations) != 3 {
		t.Fatalf("runTimed() made %d calls and returned %d durations, want 3 and 3", calls, len(durations))
	}
}

func TestRunTimedStopsOnError(t *testing.T) {
	calls := 0
	durations, err := runTimed(context.Background(), 3, false, func(context.Context) error {
		calls++
		if calls == 2 {
			return errors.New("boom")
		}
		return nil
	})
	if err == nil {
		t.Fatal("runTimed() error = nil, want error")
	}
	if calls != 2 || len(durations) != 1 {
		t.Fatalf("runTimed() made %d calls and returned %d durations, want 2 and 1", calls, len(durations))
	}
}
