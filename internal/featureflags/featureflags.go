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
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

const (
	DefaultFeatureFlagsPath = "deploy/featureflags/local.yaml"
)

// Container for feature flag values.
type Flags struct {
	EnableV3         bool    `yaml:"EnableV3"`
	V3MirrorFraction float64 `yaml:"V3MirrorFraction"`
}

// setDefaultValues creates a new Flags struct with default values.
func setDefaultValues() *Flags {
	return &Flags{
		EnableV3:         false,
		V3MirrorFraction: 0.0,
	}
}

// Creates a new Flags struct with default values,
// then overrides them with values from the config file if it is present.
func NewFlags(path string) (*Flags, error) {
	type config struct {
		Flags *Flags `yaml:"flags"`
	}

	cfg := &config{Flags: setDefaultValues()}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Feature flags file not found at %s. Using default values.", path)
			return cfg.Flags, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg.Flags, nil
}
