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

package integration

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
)

func TestGetStatVarPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setup(true)
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	for _, c := range []struct {
		id   string
		want []string
	}{
		{
			"Count_Person",
			[]string{
				"Count_Person",
				"dc/g/Variables_Demographics",
				"dc/g/Demographics",
			},
		},
		{
			"dc/g/Person_Age_ArmedForcesStatus_Employment_EmploymentStatus",
			[]string{
				"dc/g/Person_Age_ArmedForcesStatus_Employment_EmploymentStatus",
				"dc/g/Person_Age_Employment_EmploymentStatus",
				"dc/g/Person_Age_Employment",
				"dc/g/Person_Age",
				"dc/g/Demographics",
			},
		},
	} {
		resp, err := client.GetStatVarPath(ctx, &pb.GetStatVarPathRequest{
			Id: c.id,
		})
		if err != nil {
			t.Errorf("could not GetStatVarPath: %s", err)
			continue
		}

		if diff := cmp.Diff(resp.Path, c.want); diff != "" {
			t.Errorf("GetStatVarPath got diff: %v", diff)
			continue
		}
	}
}
