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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/stat"
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
) (
	*pbv1.BulkObservationDatesLinkedResponse, error) {
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
	cacheData, err := stat.ReadStatCollection(
		ctx, store.BtGroup, bigtable.BtObsCollectionDateFrequency,
		linkedEntity, entityType, variables, "",
	)
	if err != nil {
		return nil, err
	}
	for _, sv := range variables {
		data := cacheData[sv]
		if data == nil || len(data.SourceCohorts) == 0 {
			result.DatesByVariable = append(result.DatesByVariable,
				&pbv1.VariableObservationDates{
					Variable: sv,
				})
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
		tmp := &pbv1.VariableObservationDates{
			Variable:         sv,
			ObservationDates: []*pbv1.ObservationDates{},
		}
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
		result.DatesByVariable = append(result.DatesByVariable, tmp)
	}
	return result, nil
}
