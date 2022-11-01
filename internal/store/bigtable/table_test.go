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

// Defines struct and util functions of bigtable table and groups.

package bigtable

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestParseTableInfo(t *testing.T) {
	for _, c := range []struct {
		input string
		want  *pb.BigtableInfo
	}{
		{
			"project: datcom-stanford\n" +
				"instance: dc-graph\n" +
				"tables:\n" +
				"   - private_2022_10_26_16_01_13\n",
			&pb.BigtableInfo{
				Project:  "datcom-stanford",
				Instance: "dc-graph",
				Tables:   []string{"private_2022_10_26_16_01_13"},
			},
		},
	} {
		got, _ := parseTableInfo(c.input)
		if diff := cmp.Diff(got, c.want, protocmp.Transform()); diff != "" {
			t.Errorf("parseTableInfo got diff %+v", diff)
		}
	}
}
