// Copyright 2024 Google LLC
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
	"context"
	"path"
	"runtime"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNode(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}
	ds := spanner.NewSpannerDataSource(client)

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		req        *pbv2.NodeRequest
		goldenFile string
	}{
		{
			req: &pbv2.NodeRequest{
				Nodes:    []string{"Person", "Count_Person"},
				Property: "->",
			},
			goldenFile: "properties.json",
		},
		{
			req: &pbv2.NodeRequest{
				Nodes:    []string{"Monthly_Average_RetailPrice_Electricity_Residential", "Aadhaar", "foo"},
				Property: "->[typeOf, name, statType]",
			},
			goldenFile: "property_values.json",
		},
	} {
		got, err := ds.Node(ctx, c.req)
		if err != nil {
			t.Fatalf("Node error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			return
		}

		var want pbv2.NodeResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
		}
	}
}
