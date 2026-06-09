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

package golden

import (
	"bytes"
	"context"
	"log/slog"
	"path"
	"runtime"
	"strings"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
)

// Note: Tests in this file are executed sequentially within a single top-level test.
// They modify the global slog.Default() to capture and assert on logs, which makes
// them unsafe for parallel execution. Please avoid adding too many test cases here
// to prevent slowing down the test suite, as each case runs sequentially and involves
// expensive database calls.
func TestObservation_SchemaSelector(t *testing.T) {
	client := test.NewSchemaSelectorSpannerClient(t)
	ds := spanner.NewSpannerDataSource(client, nil, nil)

	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	testCases := []struct {
		desc                string
		req                 *pbv2.ObservationRequest
		useNormalizedHeader bool
		goldenFile          string
	}{
		{
			desc: "Default path - basic query",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"AirPollutant_Cancer_Risk"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/01001"},
				},
				Select: []string{"variable", "entity", "date", "value"},
			},
			useNormalizedHeader: false,
			goldenFile:          "default_obs_basic.json",
		},
		{
			desc: "Normalized path - basic query",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"AirPollutant_Cancer_Risk"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/01001"},
				},
				Select: []string{"variable", "entity", "date", "value"},
			},
			useNormalizedHeader: true,
			goldenFile:          "normalized_obs_basic.json",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			// Capture slog output
			var buf bytes.Buffer
			handler := slog.NewTextHandler(&buf, nil)
			logger := slog.New(handler)
			originalLogger := slog.Default()
			slog.SetDefault(logger)
			defer slog.SetDefault(originalLogger)

			ctx := context.Background()
			if c.useNormalizedHeader {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(util.XUseNormalizedSchema, "true"))
			}

			got, err := ds.Observation(ctx, c.req)
			if err != nil {
				t.Fatalf("Observation error: %v", err)
			}

			// Log assertions
			logStr := buf.String()
			hasLog := strings.Contains(logStr, "Invoking normalized Spanner schema")
			if c.useNormalizedHeader && !hasLog {
				t.Errorf("Expected log message 'Invoking normalized Spanner schema' not found in logs: %s", logStr)
			}
			if !c.useNormalizedHeader && hasLog {
				t.Errorf("Unexpected log message 'Invoking normalized Spanner schema' found in logs: %s", logStr)
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
				return
			}

			var want pbv2.ObservationResponse
			if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
				t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
			}
			if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
				t.Errorf("%s: %v payload mismatch:\n%v", c.desc, c.goldenFile, diff)
			}
		})
	}
}
