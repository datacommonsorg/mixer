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
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	DefaultFeatureFlagsPath = "deploy/featureflags/local.yaml"
)

// Container for feature flag values.
type Flags struct {
	// Enable datasources in V3 API.
	EnableV3 bool `yaml:"EnableV3"`
	// Fraction of V2 API requests to mirror to V3. Value from 0 to 1.0.
	V3MirrorFraction float64 `yaml:"V3MirrorFraction"`
	WriteUsageLogs   float64 `yaml:"WriteUsageLogs"`
	// Use Google Spanner as a database.
	UseSpannerGraph bool `yaml:"UseSpannerGraph"`
	// Spanner Graph database for Spanner DataSource.
	// This is temporarily loaded from flags vs spanner_graph_info.yaml.
	// TODO: Once the Spanner instance is stable, revert to using the config.
	SpannerGraphDatabase string `yaml:"SpannerGraphDatabase"`
}

// setDefaultValues creates a new Flags struct with default values.
func setDefaultValues() *Flags {
	return &Flags{
		EnableV3:             false,
		V3MirrorFraction:     0.0,
		WriteUsageLogs:       0.0,
		UseSpannerGraph:      false,
		SpannerGraphDatabase: "",
	}
}

// validateFlagValues performs any extra checks on flag value correctness.
func (f *Flags) validateFlagValues() error {
	if f.V3MirrorFraction < 0 || f.V3MirrorFraction > 1.0 {
		return fmt.Errorf("V3MirrorFraction must be between 0 and 1.0, got %f", f.V3MirrorFraction)
	}
	if f.V3MirrorFraction > 0 && !f.EnableV3 {
		return fmt.Errorf("V3MirrorFraction > 0 requires EnableV3 to be true")
	}
	if f.WriteUsageLogs < 0 || f.WriteUsageLogs > 1 {
		return fmt.Errorf("WriteUsageLogs must be between 0 and 1.0, got %f", f.WriteUsageLogs)
	}
	if f.SpannerGraphDatabase != "" && (!f.UseSpannerGraph || !f.EnableV3) {
		return fmt.Errorf("using SpannerGraphDatabase requires UseSpannerGraph and EnableV3 to be true")
	}
	return nil
}

// Creates a new Flags struct with default values,
// then overrides them with values from the config file if it is present.
func NewFlags(path string) (*Flags, error) {
	type config struct {
		Flags *Flags `yaml:"flags"`
	}

	cfg := &config{Flags: setDefaultValues()}

	if path == "" {
		// No feature flag path specified. Use default flag values.
		return cfg.Flags, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Feature flags file not found. Using default values.", "path", path)
			return cfg.Flags, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	if err := cfg.Flags.validateFlagValues(); err != nil {
		return nil, err
	}

	return cfg.Flags, nil
}
