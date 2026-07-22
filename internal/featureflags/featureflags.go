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
	"strings"

	"gopkg.in/yaml.v3"
)

// Container for feature flag values.
type Flags struct {
	// Enable datasources in V3 API.
	// Deprecated in favor of UseSpannerGraph.
	// TODO: Clean up flag once code changes roll out.
	EnableV3 bool `yaml:"EnableV3"`
	// Fraction of V2 API requests to mirror to V3. Value from 0 to 1.0.
	V3MirrorFraction float64 `yaml:"V3MirrorFraction"`
	// Enabled new Spanner-based dispatcher/datasource backend.
	UseSpannerGraph bool `yaml:"UseSpannerGraph"`
	// Whether to default Spanner API calls to the multi-entity schema.
	UseMultiEntitySchema bool `yaml:"UseMultiEntitySchema"`
	// Spanner Graph database for Spanner DataSource.
	// This is temporarily loaded from flags vs spanner_graph_info.yaml.
	// TODO: Once the Spanner instance is stable, revert to using the config.
	SpannerGraphDatabase string `yaml:"SpannerGraphDatabase"`
	// Whether to use stale reads for Spanner.
	UseStaleReads bool `yaml:"UseStaleReads"`
	// Whether to enable the embeddings resolver.
	EnableEmbeddingsResolver bool `yaml:"EnableEmbeddingsResolver"`
	// Fraction of V2 API requests to divert to the new dispatcher backend. Value from 0 to 1.0.
	V2DivertFraction float64 `yaml:"V2DivertFraction"`
	// Use inputPropertyExpressions for StatisticalCalculations to fill observation holes.
	UseStatisticalCalculation bool `yaml:"UseStatisticalCalculation"`
	// Whether to enable the SDMX API endpoint.
	EnableSDMXDataApi bool `yaml:"EnableSDMXDataApi"`
	// Maximum number of unique places returned by one remote SDMX containedInPlace+ expansion.
	SDMXRemotePlaceExpansionLimit int `yaml:"SDMXRemotePlaceExpansionLimit"`
	// Whether to default indicator resolution to Spanner.
	// If false, default requests go to legacy remote service.
	EnableSpannerSearchEmbeddings bool `yaml:"EnableSpannerSearchEmbeddings"`
	// Whether to use the new IngestionHistory schema with Timestamp.
	UseNewIngestionHistorySchema bool `yaml:"UseNewIngestionHistorySchema"`
	// Whether to read from KeyValueStore table instead of Cache table in Spanner.
	UseSpannerKeyValueStore bool `yaml:"UseSpannerKeyValueStore"`
	// Child place types whose contained-in-place queries should filter by ancestor before type.
	ContainedInPlaceAncestorFirstTypes []string `yaml:"ContainedInPlaceAncestorFirstTypes"`
	// Minimum number of unique variables that selects an entity1 range scan for core contained-in-place observation queries. Zero disables the optimization.
	ContainedInPlaceEntityScanMinVariables int `yaml:"ContainedInPlaceEntityScanMinVariables"`
}

// setDefaultValues creates a new Flags struct with default values.
func setDefaultValues() *Flags {
	return &Flags{
		EnableV3:                               false,
		V3MirrorFraction:                       0.0,
		UseSpannerGraph:                        false,
		UseMultiEntitySchema:                   false,
		SpannerGraphDatabase:                   "",
		UseStaleReads:                          false,
		EnableEmbeddingsResolver:               true,
		V2DivertFraction:                       0.0,
		UseStatisticalCalculation:              false,
		EnableSDMXDataApi:                      false,
		SDMXRemotePlaceExpansionLimit:          10000,
		EnableSpannerSearchEmbeddings:          false,
		UseNewIngestionHistorySchema:           false,
		UseSpannerKeyValueStore:                false,
		ContainedInPlaceAncestorFirstTypes:     []string{"Place"},
		ContainedInPlaceEntityScanMinVariables: 50,
	}
}

// validateFlagValues performs any extra checks on flag value correctness.
func (f *Flags) validateFlagValues() error {
	if f.V3MirrorFraction < 0 || f.V3MirrorFraction > 1.0 {
		return fmt.Errorf("V3MirrorFraction must be between 0 and 1.0, got %f", f.V3MirrorFraction)
	}
	if f.V3MirrorFraction > 0 && !f.UseSpannerGraph {
		return fmt.Errorf("V3MirrorFraction > 0 requires UseSpannerGraph to be true")
	}
	if f.SpannerGraphDatabase != "" {
		if !f.UseSpannerGraph {
			return fmt.Errorf("using SpannerGraphDatabase requires UseSpannerGraph to be true")
		}
		if strings.HasPrefix(f.SpannerGraphDatabase, "projects/") {
			parts := strings.Split(f.SpannerGraphDatabase, "/")
			if len(parts) != 6 || parts[0] != "projects" || parts[2] != "instances" || parts[4] != "databases" || parts[1] == "" || parts[3] == "" || parts[5] == "" {
				return fmt.Errorf("invalid SpannerGraphDatabase URI format: %q (expected projects/<project>/instances/<instance>/databases/<database>)", f.SpannerGraphDatabase)
			}
		}
	}
	if f.UseStaleReads && !f.UseSpannerGraph {
		return fmt.Errorf("UseStaleReads requires UseSpannerGraph to be true")
	}
	if f.V2DivertFraction < 0 || f.V2DivertFraction > 1.0 {
		return fmt.Errorf("V2DivertFraction must be between 0 and 1.0, got %f", f.V2DivertFraction)
	}
	if f.V2DivertFraction > 0 && !f.UseSpannerGraph {
		return fmt.Errorf("V2DivertFraction > 0 requires UseSpannerGraph to be true")
	}
	if f.UseSpannerKeyValueStore && !f.UseSpannerGraph {
		return fmt.Errorf("UseSpannerKeyValueStore requires UseSpannerGraph to be true")
	}
	if f.SDMXRemotePlaceExpansionLimit <= 0 {
		return fmt.Errorf("SDMXRemotePlaceExpansionLimit must be positive")
	}
	for _, placeType := range f.ContainedInPlaceAncestorFirstTypes {
		if strings.TrimSpace(placeType) == "" {
			return fmt.Errorf("ContainedInPlaceAncestorFirstTypes must not contain empty values")
		}
	}
	if f.ContainedInPlaceEntityScanMinVariables < 0 {
		return fmt.Errorf("ContainedInPlaceEntityScanMinVariables must be non-negative")
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
