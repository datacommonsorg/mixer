// Copyright 2020 Google LLC
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

package integration

import (
	"context"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatCollection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setupStatVar()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/get_stat_collection")

	for _, c := range []struct {
		parentPlace string
		childType   string
		date        string
		statVar     []string
		goldenFile  string
	}{
		// {
		// 	"geoId/06",
		// 	"County",
		// 	"",
		// 	[]string{"Count_Person", "Median_Age_Person"},
		// 	"CA_County.json",
		// },
		{
			"country/USA",
			"State",
			"",
			[]string{"Count_Person"},
			"US_State.json",
		},
		{
			"geoId/06",
			"County",
			"2016",
			[]string{"Count_Person", "Median_Age_Person"},
			"CA_County_2016.json",
		},
		{
			"country/USA",
			"County",
			"2016",
			[]string{"Count_Person"},
			"USA_County_2016.json",
		},
		{
			"country/USA",
			"City",
			"2016",
			[]string{"Count_Person"},
			"USA_City_2016.json",
		},
	} {
		resp, err := client.GetStatCollection(ctx, &pb.GetStatCollectionRequest{
			ParentPlace: c.parentPlace,
			ChildType:   c.childType,
			StatVars:    c.statVar,
			Date:        c.date,
		})
		if err != nil {
			t.Errorf("could not GetStatCollections: %s", err)
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateGolden(resp, goldenFile)
			continue
		}
		var expected pb.GetStatCollectionResponse
		file, _ := ioutil.ReadFile(goldenFile)
		err = protojson.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}

		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
