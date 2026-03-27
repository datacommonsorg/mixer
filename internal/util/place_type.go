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

package util

import (
	"log/slog"
	"os"
	"path"
	"runtime"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	placeTypes     []string
	loadPlaceTypes sync.Once
)

// GetDominantType returns the dominant type from a list of place types based on a priority list.
func GetDominantType(types []string) string {
	loadPlaceTypes.Do(func() {
		_, filename, _, _ := runtime.Caller(0)
		filePath := path.Join(path.Dir(filename), "resource/place_types.yaml")
		bytes, err := os.ReadFile(filePath)
		if err != nil {
			slog.Error("Failed to read place types config", "path", filePath, "err", err)
			return
		}
		if err := yaml.Unmarshal(bytes, &placeTypes); err != nil {
			slog.Error("Failed to unmarshal place types config", "path", filePath, "err", err)
			return
		}
	})

	// 1. Check non-place types first (PoliticalCampaignCmte, Currency)
	for _, t := range []string{"PoliticalCampaignCmte", "Currency"} {
		if StringContainedIn(t, types) {
			return t
		}
	}

	for _, pt := range placeTypes {
		if StringContainedIn(pt, types) {
			return pt
		}
	}
	return ""
}
