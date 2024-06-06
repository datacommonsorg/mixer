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

// API Implementation for /v1/bulk/observations/series/derived

// Package observations contain code for observations.
package observations

import (
	"context"
	"fmt"
	"go/token"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/protobuf/proto"
)

// DerivedSeries implements API for Mixer.DerivedObservationsSeries.
func DerivedSeries(
	ctx context.Context,
	in *pbv1.DerivedObservationsSeriesRequest,
	store *store.Store,
) (*pbv1.DerivedObservationsSeriesResponse, error) {
	resp := &pbv1.DerivedObservationsSeriesResponse{}
	entity := in.GetEntity()

	// Parse the formula to extract all the variables, used for reading data from BT.
	calculator, err := NewCalculator(in.GetFormula())
	if err != nil {
		return resp, err
	}
	statVars := calculator.StatVars()

	// Read data from BT.
	btData, err := stat.ReadStatsPb(
		ctx, store.BtGroup, []string{entity}, statVars)
	if err != nil {
		return resp, err
	}
	entityData, ok := btData[entity]
	if !ok {
		return resp, err
	}

	// Calculate.
	result, err := calculator.Calculate(
		entityData,
		extractSeriesCandidates,
		evalSeriesBinaryExpr,
		RankCalcSeries)
	if err != nil {
		return resp, err
	}
	for _, p := range result.(*CalcSeries).Points {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  p.GetDate(),
			Value: proto.Float64(p.GetValue()),
		})
	}

	return resp, nil
}

// This implements the calculatorItem interface.
type CalcSeries struct {
	FacetId string
	Facet   *pb.Facet
	// Sorted by date.
	Points []*pb.PointStat
}

// The key is concatenation of all sorted dates.
func (s *CalcSeries) Key() string {
	dates := []string{}
	for _, point := range s.Points {
		dates = append(dates, point.GetDate())
	}
	return strings.Join(dates, "")
}

func extractSeriesCandidates(
	btData interface{},
	statVar string,
	facet *pb.Facet,
) ([]CalcItem, error) {
	entityData := btData.(map[string]*pb.ObsTimeSeries)
	res := []CalcItem{}

	if obsTimeSeries, ok := entityData[statVar]; ok {
		for _, sourceSeries := range obsTimeSeries.GetSourceSeries() {
			if m := facet.GetMeasurementMethod(); m != "" {
				if m != sourceSeries.GetMeasurementMethod() {
					continue
				}
			}
			if p := facet.GetObservationPeriod(); p != "" {
				if p != sourceSeries.GetObservationPeriod() {
					continue
				}
			}
			if u := facet.GetUnit(); u != "" {
				if u != sourceSeries.GetUnit() {
					continue
				}
			}
			if s := facet.GetScalingFactor(); s != "" {
				if s != sourceSeries.GetScalingFactor() {
					continue
				}
			}
			res = append(res, toCalcSeries(sourceSeries))
		}
		if len(res) == 0 {
			return nil, fmt.Errorf("no data for %s", statVar)
		}
	} else {
		return nil, fmt.Errorf("no data for %s", statVar)
	}

	return res, nil
}

// Compute new series value of the *ast.BinaryExpr.
// Supported operations are: +, -, *, /.
func evalSeriesBinaryExpr(x, y CalcItem, op token.Token) (CalcItem, error) {
	res := &CalcSeries{Points: []*pb.PointStat{}}
	xx := x.(*CalcSeries)
	yy := y.(*CalcSeries)

	// Upper stream guarantees that x.points and y.points have same dates.
	seriesLength := len(xx.Points)

	for i := 0; i < seriesLength; i++ {
		xVal := xx.Points[i].GetValue()
		yVal := yy.Points[i].GetValue()
		var val float64
		switch op {
		case token.ADD:
			val = xVal + yVal
		case token.SUB:
			val = xVal - yVal
		case token.MUL:
			val = xVal * yVal
		case token.QUO:
			if yVal == 0 {
				return nil, fmt.Errorf("denominator cannot be zero")
			}
			val = xVal / yVal
		default:
			return nil, fmt.Errorf("unsupported op (token) %v", op)
		}
		res.Points = append(res.Points, &pb.PointStat{
			Date:  xx.Points[i].GetDate(),
			Value: proto.Float64(val),
		})
	}

	return res, nil
}

// TODO(spaceenter): Implement better ranking algorithm than simple string comparisons.
//
// The input `seriesCandidates` all have the same dates.
func RankCalcSeries(seriesCandidates []CalcItem) CalcItem {
	facetKey := func(facet *pb.Facet) string {
		return strings.Join([]string{
			facet.GetMeasurementMethod(),
			facet.GetObservationPeriod(),
			facet.GetUnit(),
			facet.GetScalingFactor()}, "-")
	}

	var res *CalcSeries
	var maxKey string
	for _, series := range seriesCandidates {
		s := series.(*CalcSeries)
		key := facetKey(s.Facet)
		if maxKey == "" || maxKey < key {
			maxKey = key
			res = s
		}
	}

	return res
}

func toCalcSeries(sourceSeries *pb.SourceSeries) *CalcSeries {
	series := &CalcSeries{
		Facet: &pb.Facet{
			MeasurementMethod: sourceSeries.GetMeasurementMethod(),
			ObservationPeriod: sourceSeries.GetObservationPeriod(),
			Unit:              sourceSeries.GetUnit(),
			ScalingFactor:     sourceSeries.GetScalingFactor(),
		},
		Points: []*pb.PointStat{},
	}

	var dates []string
	for date := range sourceSeries.GetVal() {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		series.Points = append(series.Points, &pb.PointStat{
			Date:  date,
			Value: proto.Float64(sourceSeries.GetVal()[date]),
		})
	}

	return series
}
