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
	"strings"
	"testing"
)

func TestNewMultiEntityStatementsValidatesTableConfig(t *testing.T) {
	if _, err := NewMultiEntityStatements(DefaultTableConfig()); err != nil {
		t.Fatalf("NewMultiEntityStatements(DefaultTableConfig()) returned error: %v", err)
	}

	_, err := NewMultiEntityStatements(TableConfig{})
	if err == nil {
		t.Fatal("NewMultiEntityStatements(TableConfig{}) error = nil, want error")
	}
	for _, field := range []string{
		"TimeSeriesTable",
		"ObservationTable",
		"TimeSeriesByEntity1Index",
		"TimeSeriesByEntity2Index",
		"TimeSeriesByEntity3Index",
		"TimeSeriesByProvenanceIndex",
	} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("NewMultiEntityStatements(TableConfig{}) error = %q, want missing field %q", err, field)
		}
	}

	cfg := DefaultTableConfig()
	cfg.ObservationTable = " "
	cfg.TimeSeriesByEntity2Index = ""
	_, err = NewMultiEntityStatements(cfg)
	if err == nil {
		t.Fatal("NewMultiEntityStatements(partial config) error = nil, want error")
	}
	want := "NewMultiEntityStatements: missing required TableConfig fields: ObservationTable, TimeSeriesByEntity2Index"
	if got := err.Error(); got != want {
		t.Fatalf("NewMultiEntityStatements(partial config) error = %q, want %q", got, want)
	}
}
