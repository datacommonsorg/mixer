// Copyright 2022 Google LLC
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

package stat

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/convert"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// toObsSeriesPb converts ChartStore to pb.ObsTimeSerie
func toObsSeriesPb(jsonRaw []byte) (interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := proto.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		x.ObsTimeSeries.PlaceName = ""
		ret := x.ObsTimeSeries
		// Unify unit.
		for _, series := range ret.SourceSeries {
			if conversion, ok := convert.UnitMapping[series.Unit]; ok {
				series.Unit = conversion.Unit
				for date := range series.Val {
					series.Val[date] *= conversion.Scaling
				}
			}
		}
		return ret, nil
	case nil:
		return nil, status.Error(codes.NotFound, "ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.NotFound,
			"ChartStore.Val has unexpected type %T", x)
	}
}

// toObsSeries converts ChartStore to ObsSeries
func toObsSeries(jsonRaw []byte) (interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := proto.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		pbSourceSeries := x.ObsTimeSeries.GetSourceSeries()
		ret := &model.ObsTimeSeries{
			PlaceName:    x.ObsTimeSeries.GetPlaceName(),
			SourceSeries: make([]*model.SourceSeries, len(pbSourceSeries)),
		}
		for i, source := range pbSourceSeries {
			if conversion, ok := convert.UnitMapping[source.Unit]; ok {
				source.Unit = conversion.Unit
				for date := range source.Val {
					source.Val[date] *= conversion.Scaling
				}
			}
			ret.SourceSeries[i] = &model.SourceSeries{
				ImportName:        source.GetImportName(),
				ObservationPeriod: source.GetObservationPeriod(),
				MeasurementMethod: source.GetMeasurementMethod(),
				ScalingFactor:     source.GetScalingFactor(),
				Unit:              source.GetUnit(),
				ProvenanceURL:     source.GetProvenanceUrl(),
				Val:               source.GetVal(),
			}

		}
		return ret, nil
	case nil:
		return nil, status.Error(codes.Internal, "ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.Internal, "ChartStore.Val has unexpected type %T", x)
	}
}

// toObsCollection converts ChartStore to pb.ObsCollection
func toObsCollection(jsonRaw []byte) (interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := proto.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsCollection:
		return x.ObsCollection, nil
	case nil:
		return nil, status.Error(codes.Internal, "ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.Internal,
			"ChartStore.Val has unexpected type %T", x)
	}
}

// TokenFn generates a function that convert row key to token string.
func TokenFn(keyTokens map[string]*util.PlaceStatVar) func(rowKey string) (string, error) {
	return func(rowKey string) (string, error) {
		return keyTokens[rowKey].Place + "^" + keyTokens[rowKey].StatVar, nil
	}
}

// ReadStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func ReadStats(
	ctx context.Context,
	btGroup *bigtable.Group,
	places []string,
	statVars []string,
) (map[string]map[string]*model.ObsTimeSeries, error) {
	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		bigtable.BtObsTimeSeries,
		[][]string{places, statVars},
		toObsSeries,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*model.ObsTimeSeries{}
	for _, p := range places {
		if _, ok := result[p]; !ok {
			result[p] = map[string]*model.ObsTimeSeries{}
		}
		for _, sv := range statVars {
			result[p][sv] = &model.ObsTimeSeries{}
		}
	}
	// Different base data has different source series, concatenate them together.
	for _, btData := range btDataList {
		for _, row := range btData {
			place := row.Parts[0]
			sv := row.Parts[1]
			obs := row.Data.(*model.ObsTimeSeries)
			result[place][sv].SourceSeries = append(
				result[place][sv].SourceSeries,
				obs.SourceSeries...,
			)
			result[place][sv].PlaceName = obs.PlaceName
		}
	}
	return result, nil
}

// ReadStatsPb reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func ReadStatsPb(
	ctx context.Context,
	btGroup *bigtable.Group,
	places []string,
	statVars []string,
) (map[string]map[string]*pb.ObsTimeSeries, error) {
	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		bigtable.BtObsTimeSeries,
		[][]string{places, statVars},
		toObsSeriesPb,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*pb.ObsTimeSeries{}
	// Map to record dcbranch cache data availability
	hasBranchData := map[string]struct{}{}
	for _, p := range places {
		if _, ok := result[p]; !ok {
			result[p] = map[string]*pb.ObsTimeSeries{}
		}
		for _, sv := range statVars {
			result[p][sv] = &pb.ObsTimeSeries{}
		}
	}
	for i, btData := range btDataList {
		isDcBranch := btGroup.TableNames()[i] == btGroup.BranchTableName()
		for _, row := range btData {
			place := row.Parts[0]
			sv := row.Parts[1]
			obs := row.Data.(*pb.ObsTimeSeries)
			for _, ss := range obs.SourceSeries {
				facetId := getSourceSeriesFacetID(ss)
				key := place + sv + facetId
				if _, ok := hasBranchData[key]; ok {
					continue
				}
				if isDcBranch {
					hasBranchData[key] = struct{}{}
				}
				result[place][sv].SourceSeries = append(result[place][sv].SourceSeries, ss)
			}
			result[place][sv].PlaceName = obs.PlaceName
		}
	}
	// Same sources could be from different import groups. For example, NYT Covid
	// import is included in both "frequent" and "dcbranch" group. This is to
	// collect the source with the most (latest) data.
	for p := range result {
		for sv := range result[p] {
			result[p][sv].SourceSeries = CollectDistinctSourceSeries(result[p][sv].SourceSeries)
		}
	}
	return result, nil
}

// ReadStatCollection reads and process ObsCollection cache from BigTable
// in parallel.
func ReadStatCollection(
	ctx context.Context,
	btGroup *bigtable.Group,
	prefix string,
	ancestorPlace string,
	childPlaceType string,
	statVars []string,
	date string,
) (map[string]*pb.ObsCollection, error) {
	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		prefix,
		[][]string{{ancestorPlace}, {childPlaceType}, statVars, {date}},
		toObsCollection,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsCollection{}
	for _, sv := range statVars {
		result[sv] = &pb.ObsCollection{}
	}
	// Map from facet Id to boolean to record dcbranch cache data availability
	hasBranchData := map[string]struct{}{}
	for i, btData := range btDataList {
		isDcBranch := btGroup.TableNames()[i] == btGroup.BranchTableName()
		for _, row := range btData {
			sv := row.Parts[2]
			obsCollection, ok := row.Data.(*pb.ObsCollection)
			if !ok {
				return nil, status.Errorf(codes.Internal, "invalid data for pb.ObsCollection")
			}
			for _, sc := range obsCollection.SourceCohorts {
				// Convert unit when fetching observation (not date) if possible.
				if date != "" {
					if conversion, ok := convert.UnitMapping[sc.Unit]; ok {
						sc.Unit = conversion.Unit
						for date := range sc.Val {
							sc.Val[date] *= conversion.Scaling
						}
					}
				}
				facetId := getSourceSeriesFacetID(sc)
				key := sv + facetId
				if _, ok := hasBranchData[key]; ok {
					continue
				}
				if isDcBranch {
					hasBranchData[key] = struct{}{}
				}
				result[sv].SourceCohorts = append(result[sv].SourceCohorts, sc)
			}
		}
	}
	for sv := range result {
		if len(result[sv].SourceCohorts) > 0 {
			result[sv].SourceCohorts = CollectDistinctSourceSeries(result[sv].SourceCohorts)
		} else {
			result[sv] = nil
		}
	}
	return result, nil
}
