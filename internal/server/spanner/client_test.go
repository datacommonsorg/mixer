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

package spanner

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCreateSpannerConfig(t *testing.T) {
	yamlConfig := `
project: default-project
instance: default-instance
database: default-database
`
	testCases := []struct {
		name     string
		yaml     string
		override string
		want     *SpannerConfig
		wantErr  bool
	}{
		{
			name:     "no overrides",
			yaml:     yamlConfig,
			override: "",
			want: &SpannerConfig{
				Project:  "default-project",
				Instance: "default-instance",
				Database: "default-database",
			},
			wantErr: false,
		},
		{
			name:     "override database only",
			yaml:     yamlConfig,
			override: "custom-db",
			want: &SpannerConfig{
				Project:  "default-project",
				Instance: "default-instance",
				Database: "custom-db",
			},
			wantErr: false,
		},
		{
			name:     "override with full database URI",
			yaml:     yamlConfig,
			override: "projects/uri-project/instances/uri-instance/databases/uri-db",
			want: &SpannerConfig{
				Project:  "uri-project",
				Instance: "uri-instance",
				Database: "uri-db",
			},
			wantErr: false,
		},
		{
			name:     "override with invalid database URI",
			yaml:     yamlConfig,
			override: "projects/invalid-uri",
			want:     nil,
			wantErr:  true,
		},
		{
			name:     "invalid yaml",
			yaml:     "project: [invalid-yaml",
			override: "",
			want:     nil,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := createSpannerConfig(tc.yaml, tc.override)
			if (err != nil) != tc.wantErr {
				t.Errorf("createSpannerConfig() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("createSpannerConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
