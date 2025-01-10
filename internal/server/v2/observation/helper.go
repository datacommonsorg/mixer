// Copyright 2023 Google LLC
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

// Package observation is for V2 observation API
package observation

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"google.golang.org/protobuf/proto"
)

func initObservationResult(variables []string) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}
	return result
}

func handleSQLRows(
	rows []*sqldb.Observation,
	variables []string,
) map[string]map[string]map[string][]*pb.PointStat {
	// result is keyed by variable, entity and provID
	result := make(map[string]map[string]map[string][]*pb.PointStat)
	for _, variable := range variables {
		result[variable] = make(map[string]map[string][]*pb.PointStat)
	}
	for _, row := range rows {
		entity, variable, date, prov := row.Entity, row.Variable, row.Date, row.Provenance
		value := row.Value
		if result[variable][entity] == nil {
			result[variable][entity] = map[string][]*pb.PointStat{}
		}
		if result[variable][entity][prov] == nil {
			result[variable][entity][prov] = []*pb.PointStat{}
		}
		result[variable][entity][prov] = append(
			result[variable][entity][prov],
			&pb.PointStat{
				Date:  date,
				Value: proto.Float64(value),
			},
		)
	}
	return result
}

func processSqlData(
	result *pbv2.ObservationResponse,
	mapData map[string]map[string]map[string][]*pb.PointStat,
	date string,
	sqlProvenances map[string]*pb.Facet,
) *pbv2.ObservationResponse {
	for variable := range mapData {
		for entity := range mapData[variable] {
			for provID := range mapData[variable][entity] {
				if len(mapData[variable][entity][provID]) == 0 {
					continue
				}
				obsList := mapData[variable][entity][provID]
				if date == shared.LATEST {
					obsList = obsList[len(obsList)-1:]
				}
				if result.ByVariable[variable].ByEntity[entity] == nil {
					result.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
						OrderedFacets: []*pbv2.FacetObservation{},
					}
				}
				result.ByVariable[variable].ByEntity[entity].OrderedFacets = append(
					result.ByVariable[variable].ByEntity[entity].OrderedFacets,
					&pbv2.FacetObservation{
						FacetId:      provID,
						Observations: obsList,
						ObsCount:     int32(len(obsList)),
						EarliestDate: obsList[0].Date,
						LatestDate:   obsList[len(obsList)-1].Date,
					},
				)
				result.Facets[provID] = sqlProvenances[provID]
			}
		}
	}
	return result
}
