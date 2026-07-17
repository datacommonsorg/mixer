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
		name                      string
		useSpannerGraph           bool
		flagUseSpanner           bool
		divertFraction            float64
		divertHeaderValue         string // Use "" to simulate a missing header
		disableSpannerHeaderValue string // Use "" to simulate a missing header
		expected                  bool
	}{
		{
			name:                      "Branch 1: Overall useSpannerGraph flag true forces diversion",
			useSpannerGraph:           true,
			flagUseSpanner:            false,   // Should be ignored
			divertHeaderValue:         "false", // Should be ignored
			disableSpannerHeaderValue: "",
			expected:                  true,
		},
		{
			name:                      "Branch 2: Experimental UseSpannerGraph flag false prevents diversion",
			useSpannerGraph:           false,
			flagUseSpanner:            false,
			divertHeaderValue:         "true", // Should be ignored
			disableSpannerHeaderValue: "",
			expected:                  false,
		},
		{
			name:                      "Branch 3: Header is explicitly true",
			useSpannerGraph:           false,
			flagUseSpanner:            true,
			divertHeaderValue:         "true",
			disableSpannerHeaderValue: "",
			expected:                  true,
		},
		{
			name:                      "Branch 4: Header is false, fraction is 0, returns false",
			useSpannerGraph:           false,
			flagUseSpanner:            true,
			divertHeaderValue:         "false",
			disableSpannerHeaderValue: "",
			divertFraction:            0.0,
			expected:                  false,
		},
		{
			name:                      "Branch 5: No header, fraction is 1.0 (100% diversion)",
			useSpannerGraph:           false,
			flagUseSpanner:            true,
			divertHeaderValue:         "",
			disableSpannerHeaderValue: "",
			divertFraction:            1.0, // rand.Float64() is always < 1.0
			expected:                  true,
		},
		{
			name:                      "Branch 6: Missing header, negative fraction is safely ignored",
			useSpannerGraph:           false,
			flagUseSpanner:            true,
			divertHeaderValue:         "",
			disableSpannerHeaderValue: "",
			divertFraction:            -0.5,
			expected:                  false,
		},
		{
			name:                      "Branch 7: X-Disable-Spanner overrides useSpannerGraph flag",
			useSpannerGraph:           true,
			flagUseSpanner:            false,
			divertHeaderValue:         "true",
			disableSpannerHeaderValue: "true", // Should force legacy
			expected:                  false,
		},
		{
			name:                      "Branch 8: X-Disable-Spanner overrides divert header and fraction",
			useSpannerGraph:           false,
			flagUseSpanner:            true,
			divertHeaderValue:         "true",
			disableSpannerHeaderValue: "true", // Should force legacy
			divertFraction:            1.0,
			expected:                  false,
		},
		{
			name:                      "Branch 9: X-Disable-Spanner false does not prevent diversion",
			useSpannerGraph:           true,
			flagUseSpanner:            false,
			divertHeaderValue:         "false",
			disableSpannerHeaderValue: "false", // Should NOT force legacy
			expected:                  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			md := metadata.MD{}
			if tt.divertHeaderValue != "" {
				md.Set(util.XDivertSpanner, tt.divertHeaderValue)
			}
			if tt.disableSpannerHeaderValue != "" {
				md.Set(util.XDisableSpanner, tt.disableSpannerHeaderValue)
			}
			if len(md) > 0 {
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
