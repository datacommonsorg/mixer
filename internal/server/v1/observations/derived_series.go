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
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
)

// DerivedSeries implements API for Mixer.DerivedObservationsSeries.
func DerivedSeries(
	ctx context.Context,
	in *pb.DerivedObservationsSeriesRequest,
	store *store.Store,
) (*pb.DerivedObservationsSeriesResponse, error) {
	resp := &pb.DerivedObservationsSeriesResponse{}
	entity := in.GetEntity()

	// Parse the formula to extract all the variables, used for reading data from BT.
	calculator, err := newCalculator(in.GetFormula())
	if err != nil {
		return resp, err
	}
	statVars := calculator.statVars()

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
	result, err := calculator.calculate(
		entityData,
		extractSeriesCandidates,
		evalSeriesBinaryExpr,
		rankCalcSeries)
	if err != nil {
		return resp, err
	}
	for _, p := range result.(*calcSeries).points {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  p.GetDate(),
			Value: p.GetValue(),
		})
	}

	return resp, nil
}

// This implements the calculatorItem interface.
type calcSeries struct {
	statMetadata *pb.StatMetadata
	// Sorted by date.
	points []*pb.PointStat
}

// The key is concatenation of all sorted dates.
func (s *calcSeries) key() string {
	dates := []string{}
	for _, point := range s.points {
		dates = append(dates, point.GetDate())
	}
	return strings.Join(dates, "")
}

func extractSeriesCandidates(
	btData interface{},
	statVar string,
	statMetadata *pb.StatMetadata,
) ([]calcItem, error) {
	entityData := btData.(map[string]*pb.ObsTimeSeries)
	res := []calcItem{}

	if obsTimeSeries, ok := entityData[statVar]; ok {
		for _, sourceSeries := range obsTimeSeries.GetSourceSeries() {
			if m := statMetadata.GetMeasurementMethod(); m != "" {
				if m != sourceSeries.GetMeasurementMethod() {
					continue
				}
			}
			if p := statMetadata.GetObservationPeriod(); p != "" {
				if p != sourceSeries.GetObservationPeriod() {
					continue
				}
			}
			if u := statMetadata.GetUnit(); u != "" {
				if u != sourceSeries.GetUnit() {
					continue
				}
			}
			if s := statMetadata.GetScalingFactor(); s != "" {
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
func evalSeriesBinaryExpr(x, y calcItem, op token.Token) (calcItem, error) {
	res := &calcSeries{points: []*pb.PointStat{}}
	xx := x.(*calcSeries)
	yy := y.(*calcSeries)

	// Upper stream guarantees that x.points and y.points have same dates.
	seriesLength := len(xx.points)

	for i := 0; i < seriesLength; i++ {
		xVal := xx.points[i].GetValue()
		yVal := yy.points[i].GetValue()
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
		res.points = append(res.points, &pb.PointStat{
			Date:  xx.points[i].GetDate(),
			Value: val,
		})
	}

	return res, nil
}

// TODO(spaceenter): Implement better ranking algorithm than simple string comparisons.
//
// The input `seriesCandidates` all have the same dates.
func rankCalcSeries(seriesCandidates []calcItem) calcItem {
	statMetadataKey := func(statMetadata *pb.StatMetadata) string {
		return strings.Join([]string{
			statMetadata.GetMeasurementMethod(),
			statMetadata.GetObservationPeriod(),
			statMetadata.GetUnit(),
			statMetadata.GetScalingFactor()}, "-")
	}

	var res *calcSeries
	var maxKey string
	for _, series := range seriesCandidates {
		s := series.(*calcSeries)
		key := statMetadataKey(s.statMetadata)
		if maxKey == "" || maxKey < key {
			maxKey = key
			res = s
		}
	}

	return res
}

func toCalcSeries(sourceSeries *pb.SourceSeries) *calcSeries {
	series := &calcSeries{
		statMetadata: &pb.StatMetadata{
			MeasurementMethod: sourceSeries.GetMeasurementMethod(),
			ObservationPeriod: sourceSeries.GetObservationPeriod(),
			Unit:              sourceSeries.GetUnit(),
			ScalingFactor:     sourceSeries.GetScalingFactor(),
		},
		points: []*pb.PointStat{},
	}

	var dates []string
	for date := range sourceSeries.GetVal() {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		series.points = append(series.points, &pb.PointStat{
			Date:  date,
			Value: sourceSeries.GetVal()[date],
		})
	}

	return series
}
