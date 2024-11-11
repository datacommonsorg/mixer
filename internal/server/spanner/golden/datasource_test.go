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

	v3 "github.com/datacommonsorg/mixer/internal/proto/v3"
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
	goldenFile := "node.json"

	req := &v3.NodeRequest{
		Nodes: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential"},
	}

	got, err := ds.Node(ctx, req)
	if err != nil {
		t.Fatalf("Node error (%v): %v", goldenFile, err)
	}

	if test.GenerateGolden {
		test.UpdateProtoGolden(got, goldenDir, goldenFile)
		return
	}

	var want v3.NodeResponse
	if err = test.ReadJSON(goldenDir, goldenFile, &want); err != nil {
		t.Fatalf("ReadJSON error (%v): %v", goldenFile, err)
	}

	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}
	if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
		t.Errorf("%v payload mismatch:\n%v", goldenFile, diff)
	}

}
