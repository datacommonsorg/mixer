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

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

func TestGetNodeProps(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		ids        []string
		out        bool
		goldenFile string
	}{
		{
			ids:        []string{"Count_Person", "Person"},
			out:        true,
			goldenFile: "get_node_props_by_subject_id.json",
		},
		{
			ids:        []string{"Count_Person", "Person"},
			out:        false,
			goldenFile: "get_node_props_by_object_id.json",
		},
	} {
		actual, err := client.GetNodeProps(ctx, c.ids, c.out)
		if err != nil {
			t.Fatalf("GetNodeProps error (%v): %v", c.goldenFile, err)
		}

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}
}

func TestGetNodeEdgesByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		ids        []string
		arc        *v2.Arc
		goldenFile string
	}{
		{
			ids: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "*",
			},
			goldenFile: "get_node_edges_by_subject_id.json",
		},
		{
			ids: []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "*",
			},
			goldenFile: "get_node_edges_by_object_id.json",
		},
	} {
		actual, err := client.GetNodeEdgesByID(ctx, c.ids, c.arc)
		if err != nil {
			t.Fatalf("GetNodeEdgesByID error (%v): %v", c.goldenFile, err)
		}

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}
}
func TestGetObservations(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		variables  []string
		entities   []string
		date       string
		goldenFile string
	}{
		{
			variables:  []string{"Annual_MarginOfError_Count_Person_Years16Onwards_CarTruckOrVanDroveAlone_Employed_WithEarnings"},
			entities:   []string{"geoId/01", "geoId/02"},
			date:       "",
			goldenFile: "get_observations_all_dates.json",
		},
		{
			variables:  []string{"Annual_MarginOfError_Count_Person_Years16Onwards_CarTruckOrVanDroveAlone_Employed_WithEarnings"},
			entities:   []string{"geoId/01", "geoId/02"},
			date:       "2020",
			goldenFile: "get_observations_specific_date.json",
		},
		{
			variables:  []string{"Annual_MarginOfError_Count_Person_Years16Onwards_CarTruckOrVanDroveAlone_Employed_WithEarnings"},
			entities:   []string{"geoId/01", "geoId/02"},
			date:       shared.LATEST,
			goldenFile: "get_observations_latest_date.json",
		},
	} {
		actual, err := client.GetObservations(ctx, c.variables, c.entities, c.date)
		if err != nil {
			t.Fatalf("GetObservations error (%v): %v", c.goldenFile, err)
		}

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			continue
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}

}
