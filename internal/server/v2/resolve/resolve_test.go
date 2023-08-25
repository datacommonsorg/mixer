// Copyright 2023 Google LLC
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

// Package resolve is for V2 resolve API.
package resolve

import "testing"

func TestParseCoordinate(t *testing.T) {
	for _, c := range []struct {
		coordinateExpr string
		wantLat        float64
		wantLng        float64
		wantErr        bool
	}{
		{"1.2#3.4", 1.2, 3.4, false},
		{"-1.2#abc", 0, 0, true},
		{"1.2,3.4", 0, 0, true},
	} {
		gotLat, gotLng, err := parseCoordinate(c.coordinateExpr)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseCoordinate(%s) got no error, want error",
					c.coordinateExpr)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseCoordinate(%s) = %s", c.coordinateExpr, err)
			continue
		}
		if gotLat != c.wantLat || gotLng != c.wantLng {
			t.Errorf("parseCoordinate(%s) = %f, %f, want %f, %f",
				c.coordinateExpr, gotLat, gotLng, c.wantLat, c.wantLng)
		}
	}
}
