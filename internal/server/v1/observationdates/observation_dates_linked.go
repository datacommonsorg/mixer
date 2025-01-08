// Copyright 2019 Google LLC
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

// Package observationdates holds API implementation for observation dates.

package observationdates

import (
	"context"
	"net/http"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BulkObservationDatesLinked implements API for Mixer.BulkObservationDatesLinked.
func BulkObservationDatesLinked(
	ctx context.Context,
	in *pbv1.BulkObservationDatesLinkedRequest,
	store *store.Store,
	sqlProvenances map[string]*pb.Facet,
	metadata *resource.Metadata,
	httpClient *http.Client,
) (
	*pbv1.BulkObservationDatesLinkedResponse, error,
) {
	linkedEntity := in.GetLinkedEntity()
	entityType := in.GetEntityType()
	linkedProperty := in.GetLinkedProperty()
	variables := in.GetVariables()
	if linkedEntity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: linked_entity")
	}
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: entity_type")
	}
	if len(variables) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: variables")
	}
	if linkedProperty != "containedInPlace" {
		return nil, status.Errorf(codes.InvalidArgument,
			"linked_property can only be 'containedInPlace'")
	}

	// Initialize result.
	result := &pbv1.BulkObservationDatesLinkedResponse{
		DatesByVariable: []*pbv1.VariableObservationDates{},
		Facets:          map[string]*pb.Facet{},
	}
	for _, sv := range variables {
		result.DatesByVariable = append(result.DatesByVariable,
			&pbv1.VariableObservationDates{
				Variable: sv,
			})
	}

	// Read data from BigTable.
	if store.BtGroup != nil {
		cacheData, err := stat.ReadStatCollection(
			ctx, store.BtGroup, bigtable.BtObsCollectionDateFrequency,
			linkedEntity, entityType, variables, "",
		)
		if err != nil {
			return nil, err
		}
		for idx, sv := range variables {
			data := cacheData[sv]
			if data == nil || len(data.SourceCohorts) == 0 {
				continue
			}
			// keyed by date
			datesCount := map[string][]*pbv1.EntityCount{}
			for _, cohort := range data.SourceCohorts {
				facet := util.GetFacet(cohort)
				facetID := util.GetFacetID(facet)
				for date := range cohort.Val {
					if _, ok := datesCount[date]; !ok {
						datesCount[date] = []*pbv1.EntityCount{}
					}
					datesCount[date] = append(datesCount[date], &pbv1.EntityCount{
						Count: cohort.Val[date],
						Facet: facetID,
					})
				}
				result.Facets[facetID] = facet
			}
			tmp := result.DatesByVariable[idx]
			allDates := []string{}
			for date := range datesCount {
				allDates = append(allDates, date)
			}
			sort.Strings(allDates)
			for _, date := range allDates {
				sort.SliceStable(datesCount[date], func(i, j int) bool {
					return datesCount[date][i].Count > datesCount[date][j].Count
				})
				tmp.ObservationDates = append(tmp.ObservationDates, &pbv1.ObservationDates{
					Date:        date,
					EntityCount: datesCount[date],
				})
			}
		}
	}

	// Read data from SQL store.
	if store.SQLClient.DB != nil {
		childPlaces, err := shared.FetchChildPlaces(
			ctx, store, metadata, httpClient, metadata.RemoteMixerDomain, linkedEntity, entityType)
		if err != nil {
			return nil, err
		}
		sqlResult, err := sqlquery.DateEntityCount(store.SQLClient.DB, variables, childPlaces)
		if err != nil {
			return nil, err
		}

		allProv := map[string]struct{}{}
		for _, svData := range result.DatesByVariable {
			sv := svData.Variable
			for _, dateData := range svData.ObservationDates {
				date := dateData.Date
				if _, ok := sqlResult[sv][date]; ok {
					for prov, count := range sqlResult[sv][date] {
						dateData.EntityCount = append(
							dateData.EntityCount,
							&pbv1.EntityCount{
								Count: float64(count),
								Facet: prov,
							},
						)
						allProv[prov] = struct{}{}
					}
					delete(sqlResult[sv], date)
				}
			}
			if sqlResult[sv] != nil {
				for date, countData := range sqlResult[sv] {
					newData := []*pbv1.EntityCount{}
					for prov, count := range countData {
						newData = append(
							newData,
							&pbv1.EntityCount{
								Count: float64(count),
								Facet: prov,
							},
						)
						allProv[prov] = struct{}{}
					}
					svData.ObservationDates = append(
						svData.ObservationDates,
						&pbv1.ObservationDates{
							Date:        date,
							EntityCount: newData,
						},
					)
				}
			}
			for prov := range allProv {
				result.Facets[prov] = sqlProvenances[prov]
			}
			sort.Slice(svData.ObservationDates, func(i, j int) bool {
				return svData.ObservationDates[i].Date < svData.ObservationDates[j].Date
			})
		}
	}
	return result, nil
}
