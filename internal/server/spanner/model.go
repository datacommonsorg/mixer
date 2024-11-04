// Copyright 2024 Google LLC
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

// Model objects related to the spanner graph database.
package spanner

import (
	"encoding/json"
	"fmt"
)

// Node struct representing a single row in the Node table.
type Node struct {
	ID          string  `spanner:"id"`
	TypeOf      string  `spanner:"typeOf"`
	Name        string  `spanner:"name"`
	Properties  JSONMap `spanner:"properties"`
	Provenances JSONMap `spanner:"provenances"`
}

// SpannerConfig struct to hold the YAML configuration to a spanner database.
type SpannerConfig struct {
	Project  string `yaml:"project"`
	Instance string `yaml:"instance"`
	Database string `yaml:"database"`
}

// JSONMap struct represents spanner JSON fields as golang maps.
type JSONMap struct {
	Map map[string]string
}

// Convert a JSON field to a JSONMap value.
// Note that the undecoded value happens to be a string.
func (m *JSONMap) DecodeSpanner(val interface{}) (err error) {
	strVal, ok := val.(string)
	if !ok {
		return fmt.Errorf("failed to decode JSONMap: %v", val)
	}
	if err := json.Unmarshal([]byte(strVal), &m.Map); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to map: %w", err)
	}
	return nil
}
