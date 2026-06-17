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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

type sdmxMockSpannerClient struct {
	SpannerClient

	calls  int
	result *pb.SdmxDataResult
}

func (m *sdmxMockSpannerClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	m.calls++
	return m.result, nil
}

func multiEntityHeaderContext(value string) context.Context {
	return metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(util.XUseMultiEntitySchema, value),
	)
}

func TestUseMultiEntitySchema(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		defaultOn bool
		want      bool
	}{
		{
			name: "no header uses false default",
			ctx:  context.Background(),
			want: false,
		},
		{
			name:      "no header uses true default",
			ctx:       context.Background(),
			defaultOn: true,
			want:      true,
		},
		{
			name: "true header overrides false default",
			ctx:  multiEntityHeaderContext("true"),
			want: true,
		},
		{
			name:      "false header overrides true default",
			ctx:       multiEntityHeaderContext("false"),
			defaultOn: true,
			want:      false,
		},
		{
			name:      "non true header overrides true default",
			ctx:       multiEntityHeaderContext("invalid"),
			defaultOn: true,
			want:      false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := &schemaSelectorClient{useMultiEntitySchemaFlag: tc.defaultOn}
			if got := selector.useMultiEntitySchema(tc.ctx); got != tc.want {
				t.Fatalf("selector.useMultiEntitySchema() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetSchemaNameUsesSelectedSchema(t *testing.T) {
	ctx := context.WithValue(context.Background(), selectedSchemaNameKey{}, legacySchemaName)
	if got := getSchemaName(ctx); got != legacySchemaName {
		t.Fatalf("getSchemaName() = %q, want Legacy", got)
	}

	ctx = context.WithValue(context.Background(), selectedSchemaNameKey{}, multiEntitySchemaName)
	if got := getSchemaName(ctx); got != multiEntitySchemaName {
		t.Fatalf("getSchemaName() = %q, want MultiEntity", got)
	}

	if got := getSchemaName(multiEntityHeaderContext("true")); got != legacySchemaName {
		t.Fatalf("getSchemaName() = %q, want Legacy", got)
	}
}

func TestSchemaSelectorClientCheckVariableSourceExistenceDelegatesToBaseWithMultiEntitySelection(t *testing.T) {
	selections := []struct {
		name               string
		ctx                context.Context
		useMultiEntityFlag bool
	}{
		{
			name: "header",
			ctx:  multiEntityHeaderContext("true"),
		},
		{
			name:               "feature flag",
			ctx:                context.Background(),
			useMultiEntityFlag: true,
		},
	}

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
		for _, selection := range selections {
			t.Run(selection.name+"/"+tc.name, func(t *testing.T) {
				baseClient := &sourceExistenceMockSpannerClient{rows: tc.rows}
				selector := &schemaSelectorClient{
					SpannerClient:            baseClient,
					useMultiEntitySchemaFlag: selection.useMultiEntityFlag,
				}

				got, err := selector.CheckVariableSourceExistence(selection.ctx, tc.variables, tc.sources, tc.predicate)
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
}

func TestSchemaSelectorClientGetSdmxObservations(t *testing.T) {
	for _, tc := range []struct {
		name               string
		ctx                context.Context
		useMultiEntityFlag bool
		wantBaseCalls      int
		wantMultiCalls     int
		wantCode           codes.Code
	}{
		{
			name:          "legacy default delegates to base",
			ctx:           context.Background(),
			wantBaseCalls: 1,
			wantCode:      codes.OK,
		},
		{
			name:               "multi entity flag delegates to multiEntity",
			ctx:                context.Background(),
			useMultiEntityFlag: true,
			wantMultiCalls:     1,
			wantCode:           codes.OK,
		},
		{
			name:           "true header delegates to multiEntity",
			ctx:            multiEntityHeaderContext("true"),
			wantMultiCalls: 1,
			wantCode:       codes.OK,
		},
		{
			name:               "false header overrides flag and delegates to base",
			ctx:                multiEntityHeaderContext("false"),
			useMultiEntityFlag: true,
			wantBaseCalls:      1,
			wantCode:           codes.OK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantResult := &pb.SdmxDataResult{}
			baseClient := &sdmxMockSpannerClient{result: wantResult}
			multiEntityClient := &sdmxMockSpannerClient{result: wantResult}
			selector := &schemaSelectorClient{
				SpannerClient:            baseClient,
				multiEntity:              multiEntityClient,
				useMultiEntitySchemaFlag: tc.useMultiEntityFlag,
			}

			got, err := selector.GetSdmxObservations(tc.ctx, &pb.SdmxDataQuery{})
			if code := status.Code(err); code != tc.wantCode {
				t.Fatalf("GetSdmxObservations() code = %v, want %v", code, tc.wantCode)
			}
			if baseClient.calls != tc.wantBaseCalls {
				t.Fatalf("base GetSdmxObservations calls = %d, want %d", baseClient.calls, tc.wantBaseCalls)
			}
			if multiEntityClient.calls != tc.wantMultiCalls {
				t.Fatalf("multiEntity GetSdmxObservations calls = %d, want %d", multiEntityClient.calls, tc.wantMultiCalls)
			}
			if tc.wantCode == codes.OK && got != wantResult {
				t.Fatalf("GetSdmxObservations() result = %v, want %v", got, wantResult)
			}
		})
	}
}
