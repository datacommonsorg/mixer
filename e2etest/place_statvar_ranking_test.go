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
	"net/http"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
)

type Chart struct {
	StatsVars []string `json:"statsVars"`
}

func readChartConfig() ([]Chart, error) {
	var config []Chart // quick and dirty
	resp, err := http.Get("https://raw.githubusercontent.com/datacommonsorg/website/master/server/chart_config.json")
	if err != nil {
		return config, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(body, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}

func TestChartConfigRankings(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	config, err := readChartConfig()
	if err != nil {
		t.Errorf("could not read config_statvars.txt")
		return
	}
	for _, c := range []struct {
		placeType string
	}{
		{
			"Country",
		},
		{
			"State",
		},
		{
			"County",
		},
		{
			"City",
		},
	} {
		for _, chart := range config {
			for _, sv := range chart.StatsVars {
				req := &pb.GetLocationsRankingsRequest{
					PlaceType:    c.placeType,
					StatVarDcids: []string{sv},
				}
				response, err := client.GetLocationsRankings(ctx, req)
				if err != nil || len(response.Payload) == 0 {
					t.Errorf("No rankings for %s: %s", c.placeType, sv)
					continue
				}
			}
		}
	}
}
