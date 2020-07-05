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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

type rankKey struct {
	prov    string
	mmethod string
}

// Ranking for (import name, measurement method) combination. This is used to rank
// multiple dataset for the same StatisticalVariable, where lower value means
// higher ranking.
// The ranking score ranges from 0 to 100.
var statsRanking = map[rankKey]int{
	{"CensusPEP", "CensusPEPSurvey"}:                   0, // Population
	{"CensusACS5YearSurvey", "CensusACS5yrSurvey"}:     1, // Population
	{"EurostatData", "EurostatRegionalPopulationData"}: 2, // Population
	{"WorldDevelopmentIndicators", ""}:                 3, // Population
	{"BLS_LAUS", "BLSSeasonallyUnadjusted"}:            0, // Unemployment Rate
	{"EurostatData", ""}:                               1, // Unemployment Rate
}

const lowestRank = 100

// Limit the concurrent channels when processing in-memory cache data.
const maxChannelSize = 50

// GetStats implements API for Mixer.GetStats.
func (s *Server) GetStats(ctx context.Context, in *pb.GetStatsRequest) (
	*pb.GetStatsResponse, error) {
	if len(in.GetPlace()) == 0 || in.GetStatsVar() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	// Read triples for stats var.
	triplesRowList := buildTriplesKey([]string{in.GetStatsVar()})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	statsVar, err := triplesToStatsVar(triples[in.GetStatsVar()])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList := buildStatsKey(in.GetPlace(), statsVar)

	result := map[string]*pb.ObsTimeSeries{}

	// Read data from branch in-memory cache first.
	if in.GetOption().GetCacheChoice() != pb.Option_BASE_CACHE_ONLY {
		tmp := s.memcache.ReadParallel(rowList, convertToObsSeries)
		for dcid := range tmp {
			result[dcid] = tmp[dcid].(*pb.ObsTimeSeries)
		}
	}

	// Read data from base cache if no branch cache data is found.
	// This is valid since branch cache is a superset of base cache.
	if len(result) == 0 {
		result, err = readStats(ctx, s.btTable, rowList)
	}
	if err != nil {
		return nil, err
	}

	// Fill missing place data and result result
	for _, dcid := range in.GetPlace() {
		if _, ok := result[dcid]; !ok {
			result[dcid] = nil
		}
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatsResponse{Payload: string(jsonRaw)}, nil
}

// triplesToStatsVar converts a Triples cache into a StatisticalVarible object.
func triplesToStatsVar(triples *TriplesCache) (*StatisticalVariable, error) {
	// Get constraint properties.
	propValMap := map[string]string{}
	for _, t := range triples.Triples {
		if t.Predicate == "constraintProperties" {
			propValMap[t.ObjectID] = ""
		}
	}
	statsVar := StatisticalVariable{}
	// Populate the field.
	for _, t := range triples.Triples {
		object := t.ObjectID
		switch t.Predicate {
		case "typeOf":
			if object != "StatisticalVariable" {
				return nil, fmt.Errorf("%s is not a StatisticalVariable", t.SubjectID)
			}
		case "statType":
			statsVar.StatType = strings.Replace(object, "Value", "", 1)
		case "populationType":
			statsVar.PopType = object
		case "measurementMethod":
			statsVar.MeasurementMethod = object
		case "measuredProperty":
			statsVar.MeasuredProp = object
		case "measurementDenominator":
			statsVar.MeasurementDenominator = object
		case "measurementQualifier":
			statsVar.MeasurementQualifier = object
		case "scalingFactor":
			statsVar.ScalingFactor = object
		case "unit":
			statsVar.Unit = object
		default:
			if _, ok := propValMap[t.Predicate]; ok {
				if statsVar.PVs == nil {
					statsVar.PVs = map[string]string{}
				}
				statsVar.PVs[t.Predicate] = object
			}
		}
	}
	return &statsVar, nil
}

func convertToObsSeries(dcid string, jsonRaw []byte) (interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		result := &pb.ObsTimeSeries{PlaceDcid: dcid}
		result.Unit = x.ObsTimeSeries.GetUnit()
		result.PlaceName = x.ObsTimeSeries.GetPlaceName()
		result.IsDcAggregate = x.ObsTimeSeries.GetIsDcAggregate()
		bestScore := lowestRank
		for _, series := range x.ObsTimeSeries.SourceSeries {
			key := rankKey{series.GetImportName(), series.GetMeasurementMethod()}
			score, ok := statsRanking[key]
			if !ok {
				score = lowestRank
			}
			if score <= bestScore {
				result.Data = series.Val
				bestScore = score
			}
		}
		return result, nil
	case nil:
		return nil, fmt.Errorf("ChartStore.Val is not set")
	default:
		return nil, fmt.Errorf("ChartStore.Val has unexpected type %T", x)
	}
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStats(ctx context.Context, btTable *bigtable.Table,
	rowList bigtable.RowList) (map[string]*pb.ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeries)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsTimeSeries{}
	for dcid, data := range dataMap {
		result[dcid] = data.(*pb.ObsTimeSeries)
	}
	return result, nil
}
