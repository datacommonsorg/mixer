// Copyright 2026 Google LLC
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

package dispatcher

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSdmxAvailabilityBackendUnimplemented(t *testing.T) {
	dispatcher := NewDispatcher(nil, nil)
	got, err := dispatcher.SdmxAvailability(
		context.Background(),
		&pb.SdmxAvailabilityQuery{
			ComponentId: "observationAbout",
			Constraints: map[string]*pb.ConstraintList{
				"variableMeasured": &pb.ConstraintList{Values: []string{"Count_Person"}},
			},
		},
	)

	if got != nil {
		t.Fatalf("SdmxAvailability() response = %v, want nil", got)
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
}
