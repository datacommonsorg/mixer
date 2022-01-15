// Copyright 2022 Google LLC
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

package recon

import (
	"io/ioutil"
	"path"
	"runtime"
	"testing"
)

func TestIsContainedIn(t *testing.T) {
	for _, c := range []struct {
		geoJSONFileName   string
		lat               float64
		lng               float64
		wantIsContainedIn bool
	}{
		{
			"mountain_view_geo_json.json",
			37.42,
			-122.08,
			true,
		},
		{
			"mexico_geo_json.json",
			32.41,
			-102.11,
			false,
		},
		{
			"mexico_geo_json.json",
			26.55,
			-102.85,
			true,
		},
	} {
		_, filename, _, _ := runtime.Caller(0)
		geoJSONFilePath := path.Join(
			path.Dir(filename), "test_data", c.geoJSONFileName)
		geoJSONBytes, err := ioutil.ReadFile(geoJSONFilePath)
		if err != nil {
			t.Errorf("ioutil.ReadFile(%s) = %s", c.geoJSONFileName, err)
			continue
		}
		contained, err := isContainedIn(string(geoJSONBytes), c.lat, c.lng)
		if err != nil {
			t.Errorf("isContainedIn(%s) = %s", c.geoJSONFileName, err)
			continue
		}
		if contained != c.wantIsContainedIn {
			t.Errorf("isContainedIn(%s) = %t, want %t",
				c.geoJSONFileName, contained, c.wantIsContainedIn)
		}
	}
}
