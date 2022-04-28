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

	cbt "cloud.google.com/go/bigtable"
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
		ret := x.ObsCollection
		// Unify unit.
		for _, series := range ret.SourceCohorts {
			if conversion, ok := convert.UnitMapping[series.Unit]; ok {
				series.Unit = conversion.Unit
				for date := range series.Val {
					series.Val[date] *= conversion.Scaling
				}
			}
		}
		return ret, nil
	case nil:
		return nil, status.Error(codes.Internal,
			"ChartStore.Val is not set")
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
	rowList cbt.RowList,
	keyTokens map[string]*util.PlaceStatVar,
) (map[string]map[string]*model.ObsTimeSeries, error) {
	keyToTokenFn := TokenFn(keyTokens)
	btDataList, err := bigtable.Read(
		ctx, btGroup, rowList, toObsSeries, TokenFn(keyTokens),
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*model.ObsTimeSeries{}
	for _, psv := range keyTokens {
		if _, ok := result[psv.Place]; !ok {
			result[psv.Place] = map[string]*model.ObsTimeSeries{}
		}
		if _, ok := result[psv.StatVar]; !ok {
			result[psv.Place][psv.StatVar] = &model.ObsTimeSeries{}
		}
	}
	for _, rowKey := range rowList {
		token, _ := keyToTokenFn(rowKey)
		psv := keyTokens[rowKey]
		// Different base data has different source series, concatenate them together.
		ss := result[psv.Place][psv.StatVar].SourceSeries
		for _, btData := range btDataList {
			if data, ok := btData[token]; ok {
				ss = append(
					ss,
					data.(*model.ObsTimeSeries).SourceSeries...,
				)
				result[psv.Place][psv.StatVar].PlaceName = data.(*model.ObsTimeSeries).PlaceName
			}
		}
		result[psv.Place][psv.StatVar].SourceSeries = ss
	}
	return result, nil
}

// ReadStatsPb reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func ReadStatsPb(
	ctx context.Context,
	btGroup *bigtable.Group,
	rowList cbt.RowList,
	keyTokens map[string]*util.PlaceStatVar) (
	map[string]map[string]*pb.ObsTimeSeries, error) {

	keyToTokenFn := TokenFn(keyTokens)
	btDataList, err := bigtable.Read(ctx, btGroup, rowList, toObsSeriesPb, keyToTokenFn)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*pb.ObsTimeSeries{}
	for _, psv := range keyTokens {
		if _, ok := result[psv.Place]; !ok {
			result[psv.Place] = map[string]*pb.ObsTimeSeries{}
		}
		if _, ok := result[psv.StatVar]; !ok {
			result[psv.Place][psv.StatVar] = &pb.ObsTimeSeries{}
		}
	}

	for _, rowKey := range rowList {
		token, _ := keyToTokenFn(rowKey)
		psv := keyTokens[rowKey]
		// Different base data has different source series, concatenate them together.
		ss := result[psv.Place][psv.StatVar].SourceSeries
		for _, btData := range btDataList {
			if data, ok := btData[token]; ok {
				ss = append(
					ss,
					data.(*pb.ObsTimeSeries).SourceSeries...,
				)
				result[psv.Place][psv.StatVar].PlaceName = data.(*pb.ObsTimeSeries).PlaceName
			}
		}
		// Same sources could be from different import groups. For example, NYT Covid
		// import is included in both "frequent" and "dcbranch" group. This is to
		// collect the source with the most (latest) data.
		result[psv.Place][psv.StatVar].SourceSeries = CollectDistinctSourceSeries(ss)
	}
	return result, nil
}

// ReadStatCollection reads and process ObsCollection cache from BigTable
// in parallel.
func ReadStatCollection(
	ctx context.Context,
	btGroup *bigtable.Group,
	rowList cbt.RowList,
	keyTokens map[string]string) (
	map[string]*pb.ObsCollection, error) {

	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		rowList,
		toObsCollection,
		func(rowKey string) (string, error) {
			return keyTokens[rowKey], nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsCollection{}
	for _, rowKey := range rowList {
		token := keyTokens[rowKey]
		result[token] = &pb.ObsCollection{}
		ss := result[token].SourceCohorts
		for _, btData := range btDataList {
			if data, ok := btData[token]; ok {
				obsCollection, ok := data.(*pb.ObsCollection)
				if !ok {
					return nil, status.Errorf(codes.Internal, "invalid data for pb.ObsCollection")
				}
				ss = append(ss, obsCollection.SourceCohorts...)
			}
		}
		if len(ss) > 0 {
			result[token].SourceCohorts = CollectDistinctSourceSeries(ss)
		} else {
			result[token] = nil
		}
	}
	return result, nil
}
