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

package observations

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestVarCalculatorParseFormula(t *testing.T) {
	strCmpOpts := cmpopts.SortSlices(func(a, b string) bool { return a < b })

	for _, c := range []struct {
		formula      string
		wantStatVars []string
	}{
		{
			"Person_Count_Female[u=NumberUnit;m=dcAggregate/Census;p=P1Y]/Person_Count[u=Census]",
			[]string{"Person_Count_Female", "Person_Count"},
		},
		{
			"Person_Count-Person_Count_Female-Person_Count_Male",
			[]string{"Person_Count_Female", "Person_Count", "Person_Count_Male"},
		},
		{
			"(Person_Count-Person_Count_Female) / Person_Count_Female",
			[]string{"Person_Count_Female", "Person_Count"},
		},
	} {
		varCalculator, err := newVarCalculator(c.formula)
		if err != nil {
			t.Errorf("newVarFormula(%s) = %s", c.formula, err)
		}
		gotStatVars := varCalculator.statVars()
		if diff := cmp.Diff(gotStatVars, c.wantStatVars, strCmpOpts); diff != "" {
			t.Errorf("varCalculator.statVars() diff (-want +got):\n%s", diff)
		}
	}
}

func TestParseVarName(t *testing.T) {
	for _, c := range []struct {
		varName     string
		wantVarInfo *varInfo
	}{
		{
			"Person_Count",
			&varInfo{statVar: "Person_Count"},
		},
		{
			"Person_Count_Female[u=NumberUnit;m=dcAggregate/Census;p=P1Y;s=100]",
			&varInfo{
				statVar: "Person_Count_Female",
				statMetadata: &pb.StatMetadata{
					MeasurementMethod: "dcAggregate/Census",
					ObservationPeriod: "P1Y",
					Unit:              "NumberUnit",
					ScalingFactor:     "100",
				},
			},
		},
	} {
		gotVarInfo, err := parseVarName(c.varName)
		if err != nil {
			t.Errorf("parseVarName(%s) = %s", c.varName, err)
		}
		if ok := reflect.DeepEqual(gotVarInfo, c.wantVarInfo); !ok {
			t.Errorf("parseVarName(%s) = %v, want %v",
				c.varName, gotVarInfo, c.wantVarInfo)
		}
	}
}

func TestCommonStringsAmongStringSets(t *testing.T) {
	for _, c := range []struct {
		input []map[string]struct{}
		want  map[string]struct{}
	}{
		{
			[]map[string]struct{}{
				{"a": {}, "b": {}, "c": {}},
				{"a": {}, "c": {}, "d": {}},
				{"a": {}, "c": {}, "e": {}, "f": {}},
			},
			map[string]struct{}{"a": {}, "c": {}},
		},
	} {
		got := commonStringsAmongStringSets(c.input)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("commonStringsAmongStringSets() = %v, want %v", got, c.want)
		}
	}
}
