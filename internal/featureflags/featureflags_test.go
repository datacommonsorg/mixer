// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package featureflags

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// expectedFlags creates a Flags struct with default values, applying any provided modifications.
// This ensures tests are robust to future changes in default flag values.
//
// Example usage:
//
//	want: expectedFlags(func(f *Flags) {
//		f.EnableV3 = true
//	}),
func expectedFlags(mods ...func(*Flags)) *Flags {
	f := setDefaultValues()
	for _, mod := range mods {
		mod(f)
	}
	return f
}

func TestNewFlags(t *testing.T) {
	testCases := []struct {
		name        string
		fileContent string
		want        *Flags
		wantErr     bool
	}{
		{
			name:        "file not found",
			fileContent: "", // Special case for file not found
			want:        expectedFlags(),
			wantErr:     false,
		},
		{
			name:        "invalid yaml",
			fileContent: "flags: \n  EnableV3: true\n V3MirrorFraction: 0.5", // bad indentation
			want:        nil,
			wantErr:     true,
		},
		{
			name: "partial flags",
			fileContent: `
flags:
  EnableV3: true
`,
			want: expectedFlags(func(f *Flags) {
				f.EnableV3 = true
			}),
			wantErr: false,
		},
		{
			name: "all flags",
			fileContent: `
flags:
  EnableV3: true
  V3MirrorFraction: 0.7
`,
			want: expectedFlags(func(f *Flags) {
				f.EnableV3 = true
				f.V3MirrorFraction = 0.7
			}),
			wantErr: false,
		},
		{
			name: "cluster details with flag values",
			fileContent: `
clusters:
  - projects/datcom-website-prod/locations/us-central1/clusters/website-us-central1
  - projects/datcom-website-prod/locations/us-west1/clusters/website-us-west1
flags:
  EnableV3: true
  V3MirrorFraction: 0.7
`,
			want: expectedFlags(func(f *Flags) {
				f.EnableV3 = true
				f.V3MirrorFraction = 0.7
			}),
			wantErr: false,
		},
		{
			name: "cluster details without flag values",
			fileContent: `
clusters:
  - projects/datcom-website-prod/locations/us-central1/clusters/website-us-central1
  - projects/datcom-website-prod/locations/us-west1/clusters/website-us-west1
`,
			want:    expectedFlags(),
			wantErr: false,
		},
		{
			name: "validation error - fraction too high",
			fileContent: `
flags:
  EnableV3: true
  V3MirrorFraction: 1.1
`,
			want:    nil,
			wantErr: true,
		},
		{
			name: "validation error - fraction too low",
			fileContent: `
flags:
  EnableV3: true
  V3MirrorFraction: -0.1
`,
			want:    nil,
			wantErr: true,
		},
		{
			name: "validation error - mirror without v3",
			fileContent: `
flags:
  EnableV3: false
  V3MirrorFraction: 0.5
`,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var path string
			if tc.name == "file not found" {
				path = filepath.Join(t.TempDir(), "non_existent_file.yaml")
			} else {
				tmpFile, err := os.CreateTemp(t.TempDir(), "featureflags-*.yaml")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				if _, err := tmpFile.Write([]byte(tc.fileContent)); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}
				if err := tmpFile.Close(); err != nil {
					t.Fatalf("Failed to close temp file: %v", err)
				}
				path = tmpFile.Name()
			}

			got, err := NewFlags(path)

			if (err != nil) != tc.wantErr {
				t.Errorf("NewFlags() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("NewFlags() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
