// Copyright 2021 Google LLC
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

package base

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseComplexValue(t *testing.T) {

	for _, c := range []struct {
		complexValue string
		want         string
	}{
		{
			"[dcs:Years 10 20]",
			"Years10To20",
		},
		{
			"[10 20 Years]",
			"Years10To20",
		},
		{
			"[ Years 10 20 ]",
			"Years10To20",
		},
		{
			"[dcs:Years 10 -]",
			"Years10Onwards",
		},
		{
			"[10 - dcs:Years]",
			"Years10Onwards",
		},
		{
			"[Years - 20]",
			"YearsUpto20",
		},
		{
			"[- 20 dcs:Years]",
			"YearsUpto20",
		},
		{
			"[dcs:Years 10]",
			"Years10",
		},
		{
			"[10 Years]",
			"Years10",
		},
		{
			"[LatLong 37.3884812 -122.0834373]",
			"latLong/3738848_-12208344",
		},
		{
			"[LatLong 37.3884812N 122.0834373W]",
			"latLong/3738848_-12208344",
		},
	} {
		res := ParseComplexValue(c.complexValue)
		if diff := cmp.Diff(res, c.want); diff != "" {
			t.Errorf("ParseComplexValue got diff: %v", diff)
			continue
		}
	}
}
