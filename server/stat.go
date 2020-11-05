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
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Filter a list of source series given the observation properties.
func filterSeriesPb(in []*pb.SourceSeries, prop *ObsProp) []*pb.SourceSeries {
	result := []*pb.SourceSeries{}
	for _, series := range in {
		if prop.Mmethod != "" && prop.Mmethod != series.MeasurementMethod {
			continue
		}
		if prop.Operiod != "" && prop.Operiod != series.ObservationPeriod {
			continue
		}
		if prop.Unit != "" && prop.Unit != series.Unit {
			continue
		}
		if prop.Sfactor != "" && prop.Sfactor != series.ScalingFactor {
			continue
		}
		result = append(result, series)
	}
	return result
}

func filterAndRankPb(in *pb.ObsTimeSeries, prop *ObsProp) *pb.SourceSeries {
	if in == nil {
		return nil
	}
	series := filterSeriesPb(in.SourceSeries, prop)
	sort.Sort(SeriesByRank(series))
	if len(series) > 0 {
		return series[0]
	}
	return nil
}

// GetStat implements API for Mixer.GetStat.
func (s *Server) GetStat(ctx context.Context, in *pb.GetStatRequest) (
	*pb.GetStatResponse, error) {
	placeDcids := in.GetPlaces()
	statVarDcid := in.GetStatVar()
	if len(placeDcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required argument: place")
	}
	if statVarDcid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required argument: stat_var")
	}
	filterProp := &ObsProp{
		Mmethod: in.GetMeasurementMethod(),
		Operiod: in.GetObservationPeriod(),
		Unit:    in.GetUnit(),
	}

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statVarDcid})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVarDcid] == nil {
		return nil, status.Errorf(codes.NotFound, "No statistical variable found for %s", statVarDcid)
	}
	statVarObject, err := triplesToStatsVar(statVarDcid, triples[statVarDcid])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList, keyTokens := buildStatsKey(
		placeDcids,
		map[string]*StatisticalVariable{statVarDcid: statVarObject},
	)

	tmp := map[string]*pb.ObsTimeSeries{}

	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsSeriesPb,
		tokenFn(keyTokens),
	)
	for token, data := range cacheData {
		place := strings.Split(token, "^")[0]
		if data == nil {
			tmp[place] = nil
		} else {
			tmp[place] = data.(*pb.ObsTimeSeries)
		}
	}
	// For each place, if the data is missing in branch cache, fetch it from the
	// base cache in Bigtable.
	if len(tmp) < len(placeDcids) {
		extraDcids := []string{}
		for _, dcid := range placeDcids {
			if _, ok := tmp[dcid]; !ok {
				extraDcids = append(extraDcids, dcid)
			}
		}
		rowList, keyTokens := buildStatsKey(
			extraDcids,
			map[string]*StatisticalVariable{statVarDcid: statVarObject})
		extraData, err := readStatsPb(ctx, s.btTable, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place := range extraData {
			tmp[place] = extraData[place][statVarDcid]
		}
	}

	// Fill missing place data and result result
	for _, dcid := range placeDcids {
		if _, ok := tmp[dcid]; !ok {
			tmp[dcid] = nil
		}
	}
	result := map[string]*pb.SourceSeries{}
	for place, obsSeries := range tmp {
		result[place] = filterAndRankPb(obsSeries, filterProp)
	}

	return &pb.GetStatResponse{Stat: result}, nil
}
