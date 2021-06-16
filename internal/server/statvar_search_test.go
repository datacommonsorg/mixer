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

package server

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSearchTokens(t *testing.T) {
	for _, c := range []struct {
		tokens  []string
		index   *SearchIndex
		wantSv  []string
		wantSvg []string
	}{
		{
			tokens: []string{"token1"},
			index: &SearchIndex{
				token2sv: map[string]map[string]struct{}{
					"token1": {
						"sv_1_2": {},
					},
				},
				token2svg: map[string]map[string]struct{}{
					"token1": {
						"group_1":  {},
						"group_31": {},
					},
				},
				ranking: map[string]*RankingInfo{
					"group_1": {
						ApproxNumPv: 2,
						RankingName: "token1 token2",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "token1 token3 token4",
					},
					"group_31": {
						ApproxNumPv: 2,
						RankingName: "token1 token5 token6",
					},
				},
			},
			wantSv: []string{
				"sv_1_2",
			},
			wantSvg: []string{
				"group_1", "group_31",
			},
		},
		{
			tokens: []string{"token2", "token3", "token4"},
			index: &SearchIndex{
				token2sv: map[string]map[string]struct{}{
					"token2": {
						"sv_1_1": {},
						"sv_1_2": {},
					},
					"token3": {
						"sv_1_2": {},
					},
					"token4": {
						"sv_3":   {},
						"sv_1_2": {},
					},
				},
				token2svg: map[string]map[string]struct{}{
					"token3": {
						"group_3": {},
					},
					"token4": {
						"group_3": {},
					},
				},
				ranking: map[string]*RankingInfo{
					"sv_1_1": {
						ApproxNumPv: 3,
						RankingName: "token2 token3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "token2 token3 token4",
					},
					"sv_3": {
						ApproxNumPv: 20,
						RankingName: "token4",
					},
					"group_3": {
						ApproxNumPv: 2,
						RankingName: "token2 token4 token6",
					},
				},
			},
			wantSv: []string{
				"sv_1_2",
			},
			wantSvg: []string{},
		},
	} {
		sv, svg := searchTokens(c.tokens, c.index)
		if diff := cmp.Diff(sv, c.wantSv); diff != "" {
			t.Errorf("Stat var list got diff %v", diff)
		}
		if diff := cmp.Diff(svg, c.wantSvg); diff != "" {
			t.Errorf("Stat var group list got diff %v", diff)
		}
	}
}
