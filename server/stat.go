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
	"sort"

	pb "github.com/datacommonsorg/mixer/proto"
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
