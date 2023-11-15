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
	"database/sql"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
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
	rows *sql.Rows,
	variables []string,
	queryDate string,
) (map[string]map[string][]*pb.PointStat, error) {
	tmp := make(map[string]map[string][]*pb.PointStat)
	for _, variable := range variables {
		tmp[variable] = make(map[string][]*pb.PointStat)
	}
	for rows.Next() {
		var entity, variable, date string
		var value float64
		if err := rows.Scan(&entity, &variable, &date, &value); err != nil {
			return nil, err
		}
		if tmp[variable][entity] == nil {
			tmp[variable][entity] = []*pb.PointStat{}
		}
		tmp[variable][entity] = append(tmp[variable][entity], &pb.PointStat{
			Date:  date,
			Value: proto.Float64(value),
		})
	}
	return tmp, rows.Err()
}

func processData(
	result *pbv2.ObservationResponse,
	mapData map[string]map[string][]*pb.PointStat,
	date string,
) *pbv2.ObservationResponse {
	hasData := false
	for variable := range mapData {
		for entity := range mapData[variable] {
			if len(mapData[variable][entity]) == 0 {
				continue
			}
			hasData = true
			obsList := mapData[variable][entity]
			if date == LATEST {
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
					FacetId:      "local",
					Observations: obsList,
					ObsCount:     int32(len(obsList)),
					EarliestDate: obsList[0].Date,
					LatestDate:   obsList[len(obsList)-1].Date,
				},
			)
		}
	}
	if hasData {
		result.Facets["local"] = &pb.Facet{
			ImportName:    "local",
			ProvenanceUrl: "local",
		}
	}
	return result
}
