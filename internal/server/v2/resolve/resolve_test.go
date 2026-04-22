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

import (
	"strings"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

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
		gotLat, gotLng, err := ParseCoordinate(c.coordinateExpr)
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

func TestResolveRouting(t *testing.T) {
	tests := []struct {
		desc                 string
		target               string
		hasRemoteMixerDomain bool
		wantLocal            bool
		wantRemote           bool
		wantErr              bool
	}{
		{
			desc:       "Base instance (empty remote domain)",
			target:     "any_target",
			wantLocal:  true,
			wantRemote: false,
			wantErr:    false,
		},
		{
			desc:                 "Custom instance, target base_only",
			target:               ResolveTargetBaseOnly,
			hasRemoteMixerDomain: true,
			wantLocal:            false,
			wantRemote:           true,
			wantErr:              false,
		},
		{
			desc:                 "Custom instance, target custom_only",
			target:               ResolveTargetCustomOnly,
			hasRemoteMixerDomain: true,
			wantLocal:            true,
			wantRemote:           false,
			wantErr:              false,
		},
		{
			desc:                 "Custom instance, target base_and_custom",
			target:               ResolveTargetBaseAndCustom,
			hasRemoteMixerDomain: true,
			wantLocal:            true,
			wantRemote:           true,
			wantErr:              false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			gotLocal, gotRemote := ResolveRouting(tc.target, tc.hasRemoteMixerDomain)
			if !tc.wantErr {
				if gotLocal != tc.wantLocal || gotRemote != tc.wantRemote {
					t.Errorf("resolveRouting(%q, %t) = (%v, %v), want (%v, %v)",
						tc.target, tc.hasRemoteMixerDomain, gotLocal, gotRemote, tc.wantLocal, tc.wantRemote)
				}
			}
		})
	}
}

func TestValidateAndParseResolveInputs(t *testing.T) {
	tests := []struct {
		desc             string
		in               *pbv2.ResolveRequest
		wantReq          *pbv2.ResolveRequest
		wantInProp       string
		wantOutProp      string
		wantTypeOfValues []string
		wantErr          bool
		wantErrMsg       string
	}{
		{
			desc: "all empty",
			in:   &pbv2.ResolveRequest{},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseAndCustom,
				Resolver: ResolveResolverPlace,
				Property: ResolveDefaultPropertyExpression,
			},
			wantInProp:       "description",
			wantOutProp:      "dcid",
			wantTypeOfValues: nil,
		},
		{
			desc: "partial set - target",
			in: &pbv2.ResolveRequest{
				Target: ResolveTargetCustomOnly,
			},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetCustomOnly,
				Resolver: ResolveResolverPlace,
				Property: ResolveDefaultPropertyExpression,
			},
			wantInProp:       "description",
			wantOutProp:      "dcid",
			wantTypeOfValues: nil,
		},
		{
			desc: "partial set - resolver",
			in: &pbv2.ResolveRequest{
				Resolver: ResolveResolverIndicator,
			},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseAndCustom,
				Resolver: ResolveResolverIndicator,
				Property: ResolveDefaultPropertyExpression,
			},
			wantInProp:       "description",
			wantOutProp:      "dcid",
			wantTypeOfValues: nil,
		},
		{
			desc: "fully set",
			in: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseOnly,
				Resolver: ResolveResolverPlace,
				Property: "<-wikidataId->dcid",
			},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseOnly,
				Resolver: ResolveResolverPlace,
				Property: "<-wikidataId->dcid",
			},
			wantInProp:       "wikidataId",
			wantOutProp:      "dcid",
			wantTypeOfValues: nil,
		},
		{
			desc: "typeOf filter",
			in: &pbv2.ResolveRequest{
				Property: "<-description{typeOf:City}->dcid",
			},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseAndCustom,
				Resolver: ResolveResolverPlace,
				Property: "<-description{typeOf:City}->dcid",
			},
			wantInProp:       "description",
			wantOutProp:      "dcid",
			wantTypeOfValues: []string{"City"},
		},
		{
			desc: "invalid target",
			in: &pbv2.ResolveRequest{
				Target: "invalid",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'target': valid values are 'custom_only', 'base_only', 'base_and_custom'",
		},
		{
			desc: "invalid resolver",
			in: &pbv2.ResolveRequest{
				Resolver: "invalid",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'resolver': valid values are 'indicator', 'embeddings', 'place'",
		},
		{
			desc: "invalid target and resolver",
			in: &pbv2.ResolveRequest{
				Target:   "invalid_target",
				Resolver: "invalid_resolver",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'target': valid values are 'custom_only', 'base_only', 'base_and_custom'. Invalid 'resolver': valid values are 'indicator', 'embeddings', 'place'",
		},
		{
			desc: "invalid property expression",
			in: &pbv2.ResolveRequest{
				Property: "invalid_prop",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'property' expression",
		},
		{
			desc: "unknown property for place resolver",
			in: &pbv2.ResolveRequest{
				Resolver: ResolveResolverPlace,
				Property: "<-unknown->dcid",
			},
			wantReq: &pbv2.ResolveRequest{
				Target:   ResolveTargetBaseAndCustom,
				Resolver: ResolveResolverPlace,
				Property: "<-unknown->dcid",
			},
			wantInProp:       "unknown",
			wantOutProp:      "dcid",
			wantTypeOfValues: nil,
		},
		{
			desc: "invalid property for indicator resolver (inProp)",
			in: &pbv2.ResolveRequest{
				Resolver: ResolveResolverIndicator,
				Property: "<-geoCoordinate->dcid",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'property' expression: indicator resolution only supports 'description' as input property",
		},
		{
			desc: "invalid property for indicator resolver (outProp)",
			in: &pbv2.ResolveRequest{
				Resolver: ResolveResolverIndicator,
				Property: "<-description->nutsCode",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'property' expression: indicator resolution only supports 'dcid' as output property",
		},
		{
			desc: "invalid target + valid unknown property",
			in: &pbv2.ResolveRequest{
				Target:   "invalid",
				Resolver: ResolveResolverPlace,
				Property: "<-unknown->dcid",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'target': valid values are 'custom_only', 'base_only', 'base_and_custom'",
		},
		{
			desc: "invalid output property for place (description)",
			in: &pbv2.ResolveRequest{
				Property: "<-description->nutsCode",
			},
			wantErr:    true,
			wantErrMsg: "Invalid inputs in request. Invalid 'property' expression: given input property 'description', output property can only be 'dcid'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			result, err := ValidateAndParseResolveInputs(tc.in)
			if (err != nil) != tc.wantErr {
				t.Errorf("%s validateAndParseResolveInputs() error = %v, wantErr %v", tc.desc, err, tc.wantErr)
			}
			if tc.wantErr && err != nil && !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Errorf("%s validateAndParseResolveInputs() error = %v, wantErrMsg %v", tc.desc, err, tc.wantErrMsg)
			}
			if !tc.wantErr {
				if diff := cmp.Diff(result.Request, tc.wantReq, protocmp.Transform()); diff != "" {
					t.Errorf("%s normalizedReq mismatch (-got +want):\n%s", tc.desc, diff)
				}
				if result.InProp != tc.wantInProp {
					t.Errorf("%s inProp got %s, want %s", tc.desc, result.InProp, tc.wantInProp)
				}
				if result.OutProp != tc.wantOutProp {
					t.Errorf("%s outProp got %s, want %s", tc.desc, result.OutProp, tc.wantOutProp)
				}
				if diff := cmp.Diff(result.TypeOfValues, tc.wantTypeOfValues); diff != "" {
					t.Errorf("%s typeOfValues diff (-got +want):\n%s", tc.desc, diff)
				}
			}
		})
	}
}
