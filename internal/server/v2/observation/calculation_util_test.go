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

package observation

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParseNodeName(t *testing.T) {
	for _, c := range []struct {
		nodeName string
		want     *NodeData
	}{
		{
			"Count_Person",
			&NodeData{StatVar: "Count_Person"},
		},
		{
			"Count_Person_Female[ut=NumberUnit;mm=dcAggregate/Census;op=P1Y;sf=100]",
			&NodeData{
				StatVar: "Count_Person_Female",
				Facet: &pb.Facet{
					MeasurementMethod: "dcAggregate/Census",
					ObservationPeriod: "P1Y",
					Unit:              "NumberUnit",
					ScalingFactor:     "100",
				},
			},
		},
	} {
		got, err := parseNode(c.nodeName)
		if err != nil {
			t.Errorf("parseNodeName(%s) = %s", c.nodeName, err)
		}
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("parseVarName(%s) = %v, want %v",
				c.nodeName, got, c.want)
		}
	}
}

func TestCalculatorParseFormula(t *testing.T) {
	strCmpOpts := cmpopts.SortSlices(func(a, b string) bool { return a < b })

	for _, c := range []struct {
		formula      string
		wantStatVars []string
	}{
		{
			"Person_Count_Female[ut=NumberUnit;mm=dcAggregate/Census;op=P1Y]/Person_Count[ut=Census]",
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
		calculation, err := NewCalculation(c.formula)
		if err != nil {
			t.Errorf("NewCalculation(%s) = %s", c.formula, err)
		}
		gotStatVars := calculation.StatVars
		if diff := cmp.Diff(gotStatVars, c.wantStatVars, strCmpOpts); diff != "" {
			t.Errorf("calculation.StatVars(%s) diff (-want +got):\n%s", c.formula, diff)
		}
	}
}
