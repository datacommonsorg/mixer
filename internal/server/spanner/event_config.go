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

package spanner

// RankingOrder defines the order for sorting events (Ascending or Descending).
type RankingOrder int

const (
	// ASC sorts events in ascending order of magnitude.
	ASC RankingOrder = iota
	// DESC sorts events in descending order of magnitude.
	DESC
)

// EventConfig contains the magnitude property and ordering rules for an event type.
//
// Reference: These configurations mirror the ones defined in Prophet's event_collection_manifest.textproto
// https://source.corp.google.com/piper///depot/google3/datacommons/prophet/flume_generator/event_collection_manifest.textproto
type EventConfig struct {
	// MagnitudeProp is the property used to extract the magnitude of the event (e.g., "area").
	MagnitudeProp string
	// MagnitudeValUnit is the unit of the magnitude value (e.g., "SquareKilometer").
	MagnitudeValUnit string
	// Order is the sorting order (ASC or DESC).
	Order RankingOrder
}

// EventConfigs maps event types (e.g., "DroughtEvent") to their respective ranking configurations.
//
// Notes:
// - We are currently using a compiled-in Go map for simplicity and speed.
// - In the future, this can be externalized to a JSON or YAML config file if we need to update mappings without recompiling Mixer.
// - Ideally, this configuration should be encoded in the Knowledge Graph itself (e.g., as a property of the event type class node), allowing Mixer to load it dynamically at run-time without any local config maintenance.
var EventConfigs = map[string]EventConfig{
	"DroughtEvent": {
		MagnitudeProp:    "area",
		MagnitudeValUnit: "SquareKilometer",
		Order:            DESC,
	},
	"FireEvent": {
		MagnitudeProp:    "area",
		MagnitudeValUnit: "SquareKilometer",
		Order:            DESC,
	},
	"WildlandFireEvent": {
		MagnitudeProp:    "area",
		MagnitudeValUnit: "SquareKilometer",
		Order:            DESC,
	},
	"FloodEvent": {
		MagnitudeProp:    "area",
		MagnitudeValUnit: "SquareKilometer",
		Order:            DESC,
	},
	"CycloneEvent": {
		MagnitudeProp:    "maxWindSpeed",
		MagnitudeValUnit: "Knot",
		Order:            DESC,
	},
	"WetBulbTemperatureEvent": {
		MagnitudeProp:    "wetBulbTemperature",
		MagnitudeValUnit: "Celsius",
		Order:            DESC,
	},
	"HeatTemperatureEvent": {
		MagnitudeProp:    "differenceTemperature",
		MagnitudeValUnit: "Celsius",
		Order:            DESC,
	},
	"ColdTemperatureEvent": {
		MagnitudeProp:    "differenceTemperature",
		MagnitudeValUnit: "Celsius",
		Order:            ASC,
	},
}
