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

// Container for feature flag values.
type Flags struct {
	// Enable datasources in V3 API.
	EnableV3 bool `yaml:"EnableV3"`
	// Fraction of V2 API requests to mirror to V3. Value from 0 to 1.0.
	V3MirrorFraction float64 `yaml:"V3MirrorFraction"`
	// Use Google Spanner as a database.
	UseSpannerGraph bool `yaml:"UseSpannerGraph"`
	// Spanner Graph database for Spanner DataSource.
	// This is temporarily loaded from flags vs spanner_graph_info.yaml.
	// TODO: Once the Spanner instance is stable, revert to using the config.
	SpannerGraphDatabase string `yaml:"SpannerGraphDatabase"`
	// Whether to use stale reads for Spanner.
	UseStaleReads bool `yaml:"UseStaleReads"`
	// Whether to enable the embeddings resolver.
	EnableEmbeddingsResolver bool `yaml:"EnableEmbeddingsResolver"`
}

// setDefaultValues creates a new Flags struct with default values.
func setDefaultValues() *Flags {
	return &Flags{
		EnableV3:                 false,
		V3MirrorFraction:         0.0,
		UseSpannerGraph:          false,
		SpannerGraphDatabase:     "",
		UseStaleReads:            false,
		EnableEmbeddingsResolver: true,
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
	if f.SpannerGraphDatabase != "" && (!f.UseSpannerGraph || !f.EnableV3) {
		return fmt.Errorf("using SpannerGraphDatabase requires UseSpannerGraph and EnableV3 to be true")
	}
	if f.UseStaleReads && !f.UseSpannerGraph {
		return fmt.Errorf("UseStaleReads requires UseSpannerGraph to be true")
	}
	return nil
}

func (f Flags) String() string {
	b, err := yaml.Marshal(f)
	if err != nil {
		// Use a type alias to avoid recursive String() call
		type rawFlags Flags
		return fmt.Sprintf("%+v", rawFlags(f))
	}
	return "\n" + string(b)
}

// Creates a new Flags struct with default values,
// then overrides them with values from the config file if it is present.
func NewFlags(path string) (*Flags, error) {
	type config struct {
		Flags *Flags `yaml:"flags"`
	}

	cfg := &config{Flags: setDefaultValues()}

	if path == "" {
		slog.Info("No feature flag path specified. Using default flag values.", "flags", *cfg.Flags)
		return cfg.Flags, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Feature flags file not found. Using default flag values.", "path", path, "flags", *cfg.Flags)
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

	slog.Info("Feature flags initialized from file", "path", path, "flags", *cfg.Flags)

	return cfg.Flags, nil
}
