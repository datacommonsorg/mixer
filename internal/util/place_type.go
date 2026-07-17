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
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	placeTypes        []string
	loadPlaceTypes    sync.Once
	loadPlaceTypesErr error
)

// GetDominantType returns the dominant type from a list of place types based on a priority list.
func GetDominantType(types []string) (string, error) {
	loadPlaceTypes.Do(func() {
		placeTypes, loadPlaceTypesErr = loadPlaceTypesFromFile(getPlaceTypesConfigPath())
	})
	if loadPlaceTypesErr != nil {
		return "", loadPlaceTypesErr
	}

	// 1. Check non-place types first (PoliticalCampaignCmte, Currency)
	for _, t := range []string{"PoliticalCampaignCmte", "Currency"} {
		if StringContainedIn(t, types) {
			return t, nil
		}
	}

	for _, pt := range placeTypes {
		if StringContainedIn(pt, types) {
			return pt, nil
		}
	}
	return "", nil
}

func getPlaceTypesConfigPath() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Join(path.Dir(filename), "resource/place_types.yaml")
}

func loadPlaceTypesFromFile(filePath string) ([]string, error) {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read place types config %q: %w", filePath, err)
	}

	placeTypes := []string{}
	if err := yaml.Unmarshal(bytes, &placeTypes); err != nil {
		return nil, fmt.Errorf("unmarshal place types config %q: %w", filePath, err)
	}
	return placeTypes, nil
}
