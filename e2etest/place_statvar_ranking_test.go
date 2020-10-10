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
  "io/ioutil"
  "path"
  "runtime"
  "strings"
  "testing"

  pb "github.com/datacommonsorg/mixer/proto"
  "github.com/datacommonsorg/mixer/server"
)

func TestChartConfigRankings(t *testing.T) {
  ctx := context.Background()
  client, err := setup(server.NewMemcache(map[string][]byte{}))
  if err != nil {
    t.Fatalf("Failed to set up mixer and client")
  }
  _, filename, _, _ := runtime.Caller(0)
  configStatVars := path.Join(path.Dir(filename), "config_statvars.txt")
  dat, err := ioutil.ReadFile(configStatVars)
  if err != nil {
    t.Errorf("could not read config_statvars.txt")
    return
  }
  for _, sv := range strings.Split(string(dat), "\n") {
		if sv == "" {
			continue
		}
    for _, c := range []struct {
      placeType   string
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
      req := &pb.GetLocationsRankingsRequest{
        PlaceType:    c.placeType,
        StatVarDcids: []string {sv},
      }
      response, err := client.GetLocationsRankings(ctx, req)
      if err != nil || len(response.Payload) == 0 {
        t.Errorf("No rankings for: %s %s", sv, c.placeType)
        continue
			// } else {
			// 	t.Logf("found rankings for (%s, %s)", sv, c.placeType)
			}
    }
  }
}