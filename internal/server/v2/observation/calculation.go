// Copyright 2024 Google LLC
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
	"context"
	"fmt"
	"go/token"
	"net/http"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	v1 "github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

func extractSeriesCandidates(
	data interface{},
	statVar string,
	facet *pb.Facet,
) ([]v1.CalcItem, error) {
	res := []v1.CalcItem{}
	entityData := data.(map[string][]*v1.CalcSeries)
	if candidates, ok := entityData[statVar]; ok {
		for _, candidate := range candidates {
			if m := facet.GetMeasurementMethod(); m != "" {
				if m != candidate.Facet.MeasurementMethod {
					continue
				}
			}
			if p := facet.GetObservationPeriod(); p != "" {
				if p != candidate.Facet.ObservationPeriod {
					continue
				}
			}
			if u := facet.GetUnit(); u != "" {
				if u != candidate.Facet.Unit {
					continue
				}
			}
			if s := facet.GetScalingFactor(); s != "" {
				if s != candidate.Facet.ScalingFactor {
					continue
				}
			}
			res = append(res, candidate)
		}
	} else {
		return nil, fmt.Errorf("no data for %s", statVar)
	}
	return res, nil
}

// Compute new series value of the *ast.BinaryExpr.
// Supported operations are: +, -, *, /.
func evalSeriesBinaryExpr(x, y v1.CalcItem, op token.Token) (v1.CalcItem, error) {
	res := &v1.CalcSeries{Points: []*pb.PointStat{}}
	xx := x.(*v1.CalcSeries)
	yy := y.(*v1.CalcSeries)

	// Assert that both v1.CalcSeries have the same facet.
	if xx.FacetId != yy.FacetId {
		return nil, fmt.Errorf("missaligned facets:  %v, %v", xx.FacetId, yy.FacetId)
	}

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
		res.FacetId = xx.FacetId
		res.Facet = xx.Facet
		res.Points = append(res.Points, &pb.PointStat{
			Date:  xx.Points[i].GetDate(),
			Value: proto.Float64(val),
		})
	}

	return res, nil
}

// Supposed to mirror v1.CalcSeries.Key()
func dateKey(points []*pb.PointStat) string {
	dates := []string{}
	for _, point := range points {
		dates = append(dates, point.GetDate())
	}
	return strings.Join(dates, "")
}

func computeCalculation(
	calculator *v1.Calculator,
	sv string,
	inputResp *pbv2.ObservationResponse,
) (*pbv2.ObservationResponse, error) {
	// Create map of entity -> valid facets
	// (i.e. facets that have data for the same dates for all variables).
	// This is to ensure chooseItem picks the same facet for each variable.
	// TODO: Handle facets better.
	entityList := map[string][][]string{}
	facetDates := map[string]string{}
	for _, sv := range calculator.StatVars() {
		variableObservation, ok := inputResp.ByVariable[sv]
		if !ok {
			return nil, fmt.Errorf("no data for %s", sv)
		}
		for entity, entityObservation := range variableObservation.ByEntity {
			facets := []string{}
			for _, obs := range entityObservation.OrderedFacets {
				facetKey := dateKey(obs.Observations)
				if dates, ok := facetDates[obs.FacetId]; ok {
					if dates == facetKey {
						facets = append(facets, obs.FacetId)
					}
				} else {
					facetDates[obs.FacetId] = facetKey
					facets = append(facets, obs.FacetId)
				}
			}
			entityList[entity] = append(entityList[entity], facets)
		}
	}
	entityFacets := map[string]map[string]bool{}
	for entity, list := range entityList {
		entityFacets[entity] = map[string]bool{}
		if len(list) != len(calculator.StatVars()) {
			continue
		}
		facets := util.StringListIntersection(list)
		for _, facet := range facets {
			entityFacets[entity][facet] = true
		}
	}

	// Entity -> SV -> v1.CalcSeries for valid facets.
	formattedInput := map[string]map[string][]*v1.CalcSeries{}
	for variable, variableObservation := range inputResp.ByVariable {
		for entity, entityObservation := range variableObservation.ByEntity {
			if _, ok := formattedInput[entity]; !ok {
				formattedInput[entity] = map[string][]*v1.CalcSeries{}
			}
			for _, obs := range entityObservation.OrderedFacets {
				if _, ok := entityFacets[entity][obs.FacetId]; ok {
					formattedInput[entity][variable] = append(
						formattedInput[entity][variable],
						&v1.CalcSeries{
							FacetId: obs.FacetId,
							Facet:   inputResp.Facets[obs.FacetId],
							Points:  obs.Observations,
						},
					)
				}
			}
		}
	}

	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			sv: {},
		},
		Facets: map[string]*pb.Facet{},
	}
	for entity, entityData := range formattedInput {
		resp, err := calculator.Calculate(
			entityData,
			extractSeriesCandidates,
			evalSeriesBinaryExpr,
			v1.RankCalcSeries,
		)
		if err != nil {
			continue
		}
		calcSeries := resp.(*v1.CalcSeries)
		if len(result.ByVariable[sv].ByEntity) == 0 {
			result.ByVariable[sv].ByEntity = map[string]*pbv2.EntityObservation{}
		}
		result.ByVariable[sv].ByEntity[entity] = &pbv2.EntityObservation{
			OrderedFacets: []*pbv2.FacetObservation{{
				FacetId:      calcSeries.FacetId,
				Observations: calcSeries.Points,
				EarliestDate: calcSeries.Points[0].GetDate(),
				LatestDate:   calcSeries.Points[len(calcSeries.Points)-1].GetDate(),
				ObsCount:     int32(len(calcSeries.Points)),
			}},
		}
		if _, ok := result.Facets[calcSeries.FacetId]; !ok {
			result.Facets[calcSeries.FacetId] = &pb.Facet{
				ImportName:        calcSeries.Facet.ImportName,
				ProvenanceUrl:     calcSeries.Facet.ProvenanceUrl,
				MeasurementMethod: calcSeries.Facet.MeasurementMethod,
				ObservationPeriod: calcSeries.Facet.ObservationPeriod,
				ScalingFactor:     calcSeries.Facet.ScalingFactor,
				Unit:              calcSeries.Facet.Unit,
				IsDcAggregate:     calcSeries.Facet.IsDcAggregate,
				IsDcImputed:       calcSeries.Facet.IsDcImputed,
			}
		}
	}
	return result, nil
}

// CalculateObservationResponses returns a list of ObservationResponses based
// on StatisticalCalculation formulas for the missing data in an input
// ObservationResponse.
func CalculateObservationResponses(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	inputReq *pbv2.ObservationRequest,
	inputResp *pbv2.ObservationResponse,
) []*pbv2.ObservationResponse {
	calculatedResps := []*pbv2.ObservationResponse{}
	// Currently do not support nested formulas.
	if inputReq.Variable.GetFormula() != "" {
		return calculatedResps
	}
	for variable, variableObservation := range inputResp.ByVariable {
		formulas, ok := cachedata.SVFormula()[variable]
		if !ok {
			continue
		}
		for _, formula := range formulas {
			calculator, err := v1.NewCalculator(formula)
			if err != nil {
				continue
			}
			entityReq := &pbv2.DcidOrExpression{}
			if len(inputReq.Entity.GetDcids()) > 0 {
				requestedEntities := []string{}
				for entity, entityObservation := range variableObservation.ByEntity {
					// inputResp already has data, so no need to calculate.
					if len(entityObservation.OrderedFacets) != 0 {
						continue
					}
					requestedEntities = append(requestedEntities, entity)
				}
				if len(requestedEntities) == 0 {
					break
				}
				entityReq = &pbv2.DcidOrExpression{Dcids: requestedEntities}
			}
			if inputReq.Entity.GetExpression() != "" {
				if len(variableObservation.ByEntity) != 0 {
					// inputResp already has data, so no need to calculate.
					break
				}
				entityReq = &pbv2.DcidOrExpression{Expression: inputReq.Entity.Expression}
			}
			svResp, err := ObservationInternal(ctx, store, cachedata, metadata, httpClient, &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: calculator.StatVars()},
				Entity:   entityReq,
				Date:     inputReq.Date,
				Value:    inputReq.Value,
				Filter:   inputReq.Filter,
				Select:   inputReq.Select,
			})
			if err != nil {
				continue
			}
			calculatedResp, err := computeCalculation(calculator, variable, svResp)
			if err != nil {
				continue
			}
			calculatedResps = append(calculatedResps, calculatedResp)
			break
		}
	}
	return calculatedResps
}
