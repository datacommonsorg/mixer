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

package restv2

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInternalConstraintComponentID(t *testing.T) {
	tests := []struct {
		name      string
		component string
		want      string
		wantCode  codes.Code
	}{
		{
			name:      "time period maps to internal observation date",
			component: "TIME_PERIOD",
			want:      "observationDate",
			wantCode:  codes.OK,
		},
		{
			name:      "variable measured passes through",
			component: "variableMeasured",
			want:      "variableMeasured",
			wantCode:  codes.OK,
		},
		{
			name:      "observation about passes through",
			component: "observationAbout",
			want:      "observationAbout",
			wantCode:  codes.OK,
		},
		{
			name:      "observation value is unsupported",
			component: "OBS_VALUE",
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "arbitrary component is unsupported",
			component: "GEO",
			wantCode:  codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InternalConstraintComponentID(tt.component)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("InternalConstraintComponentID() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
			}
			if got != tt.want {
				t.Errorf("InternalConstraintComponentID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInternalAvailabilityComponentID(t *testing.T) {
	tests := []struct {
		name      string
		component string
		want      string
		wantCode  codes.Code
	}{
		{
			name:      "observation about passes through",
			component: "observationAbout",
			want:      "observationAbout",
			wantCode:  codes.OK,
		},
		{
			name:      "provenance passes through",
			component: "provenance",
			want:      "provenance",
			wantCode:  codes.OK,
		},
		{
			name:      "unit passes through",
			component: "unit",
			want:      "unit",
			wantCode:  codes.OK,
		},
		{
			name:      "measurement method passes through",
			component: "measurementMethod",
			want:      "measurementMethod",
			wantCode:  codes.OK,
		},
		{
			name:      "observation period passes through",
			component: "observationPeriod",
			want:      "observationPeriod",
			wantCode:  codes.OK,
		},
		{
			name:      "variable measured passes through",
			component: "variableMeasured",
			want:      "variableMeasured",
			wantCode:  codes.OK,
		},
		{
			name:      "time period is unsupported",
			component: "TIME_PERIOD",
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "observation value is unsupported",
			component: "OBS_VALUE",
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "attribute is unsupported",
			component: "scalingFactor",
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "unknown component is unsupported",
			component: "GEO",
			wantCode:  codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InternalAvailabilityComponentID(tt.component)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("InternalAvailabilityComponentID() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
			}
			if got != tt.want {
				t.Errorf("InternalAvailabilityComponentID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInternalAvailabilityConstraintComponentID(t *testing.T) {
	tests := []struct {
		name      string
		component string
		want      string
		wantCode  codes.Code
	}{
		{
			name:      "variable measured passes through",
			component: "variableMeasured",
			want:      "variableMeasured",
			wantCode:  codes.OK,
		},
		{
			name:      "observation about is unsupported",
			component: "observationAbout",
			wantCode:  codes.Unimplemented,
		},
		{
			name:      "time period is unsupported",
			component: "TIME_PERIOD",
			wantCode:  codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InternalAvailabilityConstraintComponentID(tt.component)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("InternalAvailabilityConstraintComponentID() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
			}
			if got != tt.want {
				t.Errorf("InternalAvailabilityConstraintComponentID() = %q, want %q", got, tt.want)
			}
		})
	}
}
