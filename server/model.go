// Copyright 2020 Google LLC
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

package server

import (
	"github.com/datacommonsorg/mixer/base"
	"github.com/datacommonsorg/mixer/translator"
)

// Triple represents a triples entry in the BT triples cache.
type Triple struct {
	SubjectID    string   `json:"subjectId,omitempty"`
	SubjectName  string   `json:"subjectName,omitempty"`
	SubjectTypes []string `json:"subjectTypes,omitempty"`
	Predicate    string   `json:"predicate,omitempty"`
	ObjectID     string   `json:"objectId,omitempty"`
	ObjectName   string   `json:"objectName,omitempty"`
	ObjectValue  string   `json:"objectValue,omitempty"`
	ObjectTypes  []string `json:"objectTypes,omitempty"`
	ProvenanceID string   `json:"provenanceId,omitempty"`
}

// Node represents a information about a node.
type Node struct {
	Dcid   string   `json:"dcid,omitempty"`
	Name   string   `json:"name,omitempty"`
	ProvID string   `json:"provenanceId,omitempty"`
	Value  string   `json:"value,omitempty"`
	Types  []string `json:"types,omitempty"`
}

// TriplesCache represents the json structure returned by the BT triples cache
type TriplesCache struct {
	Triples []*Triple `json:"triples"`
}

// PropValueCache represents the json structure returned by the BT PropVal cache
type PropValueCache struct {
	Nodes []*Node `json:"entities,omitempty"`
}

// PropLabelCache represents the json structure returned by the BT Prop cache
type PropLabelCache struct {
	InLabels  []string `json:"inLabels"`
	OutLabels []string `json:"outLabels"`
}

type chanData struct {
	dcid string
	data interface{}
}

// RelatedPlacesInfo represents the json structure returned by the RelatedPlaces cache.
type RelatedPlacesInfo struct {
	RelatedPlaces  []string `json:"relatedPlaces,omitempty"`
	RankFromTop    int32    `json:"rankFromTop,omitempty"`
	RankFromBottom int32    `json:"rankFromBottom,omitempty"`
}

// StatisticalVariable contains key info of population and observation.
type StatisticalVariable struct {
	PopType                string            `json:"popType,omitempty"`
	PVs                    map[string]string `json:"pvs,omitempty"`
	MeasuredProp           string            `json:"measuredProp,omitempty"`
	MeasurementMethod      string            `json:"measurementMethod,omitempty"`
	MeasurementDenominator string            `json:"measurementDeonominator,omitempty"`
	MeasurementQualifier   string            `json:"measurementQualifier,omitempty"`
	ScalingFactor          string            `json:"scalingFactor,omitempty"`
	Unit                   string            `json:"unit,omitempty"`
	StatType               string            `json:"statType,omitempty"`
}

// PlacePopInfo contains basic info for a place and a population.
type PlacePopInfo struct {
	PlaceID      string `json:"dcid,omitempty"`
	PopulationID string `json:"population,omitempty"`
}

//PlaceStatsVar contains the place stats var list.
type PlaceStatsVar struct {
	StatVarIds []string `json:"statVarIds,omitempty"`
}

// Metadata represents the metadata used by the server.
type Metadata struct {
	Mappings   []*base.Mapping
	OutArcInfo map[string]map[string][]translator.OutArcInfo
	InArcInfo  map[string][]translator.InArcInfo
	SubTypeMap map[string]string
	Bq         string
}

// SourceSeries represents time series data for a particular source.
type SourceSeries struct {
	ImportName        string             `json:"importName,omitempty"`
	ObservationPeriod string             `json:"observationPeriod,omitempty"`
	MeasurementMethod string             `json:"measurementMethod,omitempty"`
	ScalingFactor     string             `json:"scalingFactor,omitempty"`
	Unit              string             `json:"unit,omitempty"`
	ProvenanceDomain  string             `json:"provenanceDomain,omitempty"`
	Val               map[string]float64 `json:"val,omitempty"`
}

// ObsTimeSeries repesents multiple time series data.
type ObsTimeSeries struct {
	Data             map[string]float64 `json:"data,omitempty"`
	PlaceName        string             `json:"placeName,omitempty"`
	PlaceDcid        string             `json:"placeDcid,omitempty"`
	SourceSeries     []*SourceSeries    `json:"sourceSeries,omitempty"`
	ProvenanceDomain string             `json:"provenanceDomain,omitempty"`
}

// LandingPageData represents the landing page cache data.
type LandingPageData struct {
	Data map[string]*ObsTimeSeries `json:"data,omitempty"`
}

type place struct {
	Dcid string `json:"dcid,omitempty"`
	Name string `json:"name,omitempty"`
	Pop  int32  `json:"pop,omitempty"`
}

// LandingPageResponse represents all the data needed for rendering landing page.
type LandingPageResponse struct {
	Data            map[string]map[string]*ObsTimeSeries `json:"data,omitempty"`
	AllChildPlaces  map[string][]*place                  `json:"allChildPlaces,omitempty"`
	ChildPlacesType string                               `json:"childPlacesType,omitempty"`
	ChildPlaces     []string                             `json:"childPlaces,omitempty"`
	ParentPlaces    []string                             `json:"parentPlaces,omitempty"`
	SimilarPlaces   []string                             `json:"similarPlaces,omitempty"`
	NearbyPlaces    []string                             `json:"nearbyPlaces,omitempty"`
}
