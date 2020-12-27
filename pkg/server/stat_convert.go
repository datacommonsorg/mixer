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
	pb "github.com/datacommonsorg/mixer/pkg/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// convert ChartStore to pb.ObsTimeSerie
func convertToObsSeriesPb(token string, jsonRaw []byte) (
	interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		x.ObsTimeSeries.PlaceName = ""
		return x.ObsTimeSeries, nil
	case nil:
		return nil, status.Error(codes.NotFound, "ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.NotFound,
			"ChartStore.Val has unexpected type %T", x)
	}
}

// convert ChartStore to ObsSeries
func convertToObsSeries(token string, jsonRaw []byte) (
	interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		pbSourceSeries := x.ObsTimeSeries.GetSourceSeries()
		ret := &ObsTimeSeries{
			Data:         x.ObsTimeSeries.GetData(),
			PlaceName:    x.ObsTimeSeries.GetPlaceName(),
			SourceSeries: make([]*SourceSeries, len(pbSourceSeries)),
		}
		for i, source := range pbSourceSeries {
			ret.SourceSeries[i] = &SourceSeries{
				ImportName:        source.GetImportName(),
				ObservationPeriod: source.GetObservationPeriod(),
				MeasurementMethod: source.GetMeasurementMethod(),
				ScalingFactor:     source.GetScalingFactor(),
				Unit:              source.GetUnit(),
				ProvenanceDomain:  source.GetProvenanceDomain(),
				ProvenanceURL:     source.GetProvenanceUrl(),
				Val:               source.GetVal(),
			}
		}
		ret.ProvenanceDomain = x.ObsTimeSeries.GetProvenanceDomain()
		ret.ProvenanceURL = x.ObsTimeSeries.GetProvenanceUrl()
		return ret, nil
	case nil:
		return nil, status.Error(codes.Internal, "ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.Internal, "ChartStore.Val has unexpected type %T", x)
	}
}

// convert ChartStore to pb.ObsCollection
func convertToObsCollection(token string, jsonRaw []byte) (
	interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsCollection:
		return x.ObsCollection, nil
	case nil:
		return nil, status.Error(codes.Internal,
			"ChartStore.Val is not set")
	default:
		return nil, status.Errorf(codes.Internal,
			"ChartStore.Val has unexpected type %T", x)
	}
}

// Use func closure to wrap date.
func makeFnConvertToPointStat(date string) func(token string, jsonRaw []byte) (
	interface{}, error) {
	// convert ChartStore to pb.PointStat
	// This get the highest ranked source series and convert it to PointStat with
	// the latest value.
	return func(token string, jsonRaw []byte) (
		interface{}, error) {
		pbData := &pb.ChartStore{}
		if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
			return nil, err
		}
		switch x := pbData.Val.(type) {
		case *pb.ChartStore_ObsTimeSeries:
			pointStat, err := getValueFromBestSourcePb(x.ObsTimeSeries, date)
			if err != nil {
				return nil, err
			}
			return pointStat, nil
		case nil:
			return nil, status.Error(codes.NotFound, "ChartStore.Val is not set")
		default:
			return nil, status.Errorf(codes.NotFound,
				"ChartStore.Val has unexpected type %T", x)
		}
	}
}
