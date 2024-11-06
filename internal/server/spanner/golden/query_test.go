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

	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

func TestGetNodeEdgesByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")
	goldenFile := "get_node_edges_by_id.json"

	ids := []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential"}

	actual, err := client.GetNodeEdgesByID(ctx, ids)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID error (%v): %v", goldenFile, err)
	}

	got, err := test.StructToJSON(actual)
	if err != nil {
		t.Fatalf("StructToJSON error (%v): %v", goldenFile, err)
	}

	if test.GenerateGolden {
		err = test.WriteGolden(got, goldenDir, goldenFile)
		if err != nil {
			t.Fatalf("WriteGolden error (%v): %v", goldenFile, err)
		}
		return
	}

	want, err := test.ReadGolden(goldenDir, goldenFile)
	if err != nil {
		t.Fatalf("ReadGolden error (%v): %v", goldenFile, err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("%v payload mismatch (-want +got):\n%s", goldenFile, diff)
	}

}
