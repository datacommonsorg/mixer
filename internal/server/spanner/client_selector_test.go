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
	"context"
	"reflect"
	"testing"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

type sourceExistenceMockSpannerClient struct {
	SpannerClient

	calls        int
	gotVariables []string
	gotSources   []string
	gotPredicate string
	rows         [][]string
}

func (m *sourceExistenceMockSpannerClient) CheckVariableSourceExistence(ctx context.Context, variables []string, sources []string, predicate string) ([][]string, error) {
	m.calls++
	m.gotVariables = append([]string(nil), variables...)
	m.gotSources = append([]string(nil), sources...)
	m.gotPredicate = predicate
	return m.rows, nil
}

func TestSchemaSelectorClientCheckVariableSourceExistenceDelegatesToBaseWithMultiEntityHeader(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(util.XUseMultiEntitySchema, "true"),
	)

	for _, tc := range []struct {
		name      string
		variables []string
		sources   []string
		predicate string
		rows      [][]string
	}{
		{
			name:      "stat vars and sources",
			variables: []string{"Count_Person"},
			sources:   []string{"dc/base/TestImport"},
			rows:      [][]string{{"Count_Person", "dc/base/TestImport"}},
		},
		{
			name:      "variable groups and sources",
			variables: []string{"dc/g/TestGroup"},
			sources:   []string{"dc/base/TestImport"},
			predicate: "linkedMemberOf",
			rows:      [][]string{{"dc/g/TestGroup", "dc/base/TestImport"}},
		},
		{
			name:      "topics and sources",
			variables: []string{"dc/t/TestTopic"},
			sources:   []string{"dc/base/TestImport"},
			predicate: "linkedMember",
			rows:      [][]string{{"dc/t/TestTopic", "dc/base/TestImport"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseClient := &sourceExistenceMockSpannerClient{rows: tc.rows}
			selector := &schemaSelectorClient{
				SpannerClient: baseClient,
			}

			got, err := selector.CheckVariableSourceExistence(ctx, tc.variables, tc.sources, tc.predicate)
			if err != nil {
				t.Fatalf("CheckVariableSourceExistence returned error: %v", err)
			}
			if baseClient.calls != 1 {
				t.Fatalf("base CheckVariableSourceExistence calls = %d, want 1", baseClient.calls)
			}
			if !reflect.DeepEqual(got, tc.rows) {
				t.Fatalf("CheckVariableSourceExistence rows = %v, want %v", got, tc.rows)
			}
			if !reflect.DeepEqual(baseClient.gotVariables, tc.variables) {
				t.Fatalf("variables = %v, want %v", baseClient.gotVariables, tc.variables)
			}
			if !reflect.DeepEqual(baseClient.gotSources, tc.sources) {
				t.Fatalf("sources = %v, want %v", baseClient.gotSources, tc.sources)
			}
			if baseClient.gotPredicate != tc.predicate {
				t.Fatalf("predicate = %q, want %q", baseClient.gotPredicate, tc.predicate)
			}
		})
	}
}
