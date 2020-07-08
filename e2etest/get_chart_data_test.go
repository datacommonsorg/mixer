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

package e2etest

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetChartData(t *testing.T) {
	ctx := context.Background()

	memcacheData, err := loadMemcache()
	if err != nil {
		t.Fatalf("Failed to load memcache %v", err)
	}

	client, err := setup(server.NewMemcache(memcacheData))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenFile := path.Join(
		path.Dir(filename), "../golden_response/staging/get_chart_data.json")

	keys := []string{
		"country/ITA^count^measured^^^^Person",
		"geoId/06^count^measured^^^^Person",
		"nces/062631003930^count^measured^^^^Student",
		"nces/062631003930^count^measured^^^^Student^gender^Female",
		"geoId/06085^cumulativeCount^measured^^^^MedicalConditionIncident^incidentType^COVID_19^medicalStatus^ConfirmedOrProbableCase",
	}
	req := &pb.GetChartDataRequest{
		Keys: keys,
	}
	resp, err := client.GetChartData(ctx, req)
	if err != nil {
		t.Fatalf("could not GetChartData: %s", err)
	}
	var result map[string]*pb.ObsTimeSeries
	err = json.Unmarshal([]byte(resp.GetPayload()), &result)
	if err != nil {
		t.Fatalf("Can not Unmarshal payload: %v", err)
	}
	var expected map[string]*pb.ObsTimeSeries
	file, _ := ioutil.ReadFile(goldenFile)
	err = json.Unmarshal(file, &expected)
	if err != nil {
		t.Fatalf("Can not Unmarshal golden file %s: %v", goldenFile, err)
	}
	if diff := cmp.Diff(result, expected, protocmp.Transform()); diff != "" {
		t.Errorf("payload got diff: %v", diff)
	}
}
