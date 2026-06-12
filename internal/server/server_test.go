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

package server

import (
	"context"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

func TestShouldDivertV2(t *testing.T) {

	tests := []struct {
		name            string
		useSpannerGraph bool
		flagUseSpanner  bool
		divertFraction  float64
		headerValue     string // Use "" to simulate a missing header
		expected        bool
	}{
		{
			name:            "Branch 1: Overall useSpannerGraph flag true forces diversion",
			useSpannerGraph: true,
			flagUseSpanner:  false,   // Should be ignored
			headerValue:     "false", // Should be ignored
			expected:        true,
		},
		{
			name:            "Branch 2: Experimental UseSpannerGraph flag false prevents diversion",
			useSpannerGraph: false,
			flagUseSpanner:  false,
			headerValue:     "true", // Should be ignored
			expected:        false,
		},
		{
			name:            "Branch 3: Header is explicitly true",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "true",
			expected:        true,
		},
		{
			name:            "Branch 4: Header is false, fraction is 0, returns false",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "false",
			divertFraction:  0.0,
			expected:        false,
		},
		{
			name:            "Branch 5: No header, fraction is 1.0 (100% diversion)",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "",
			divertFraction:  1.0, // rand.Float64() is always < 1.0
			expected:        true,
		},
		{
			name:            "Branch 6: Missing header, negative fraction is safely ignored",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "",
			divertFraction:  -0.5,
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.headerValue != "" {
				md := metadata.Pairs(util.XDivertSpanner, tt.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			s := &Server{
				useSpannerGraph: tt.useSpannerGraph,
				flags: &featureflags.Flags{
					UseSpannerGraph:  tt.flagUseSpanner,
					V2DivertFraction: tt.divertFraction,
				},
			}

			got := s.shouldDivertV2(ctx)
			if got != tt.expected {
				t.Errorf("shouldDivertV2() = %v, want %v", got, tt.expected)
			}
		})
	}
}
