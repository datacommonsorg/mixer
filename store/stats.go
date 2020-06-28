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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"github.com/golang/protobuf/jsonpb"
	"golang.org/x/sync/errgroup"
)

type rankKey struct {
	prov    string
	mmethod string
}

// Ranking for (import name, measurement method) combination. This is used to rank
// multiple dataset for the same StatisticalVariable, where lower value means
// higher ranking.
// The ranking score ranges from 0 to 100.
var statsRanking = map[rankKey]int{
	{"CensusPEP", "CensusPEPSurvey"}:                   0, // Population
	{"CensusACS5YearSurvey", "CensusACS5yrSurvey"}:     1, // Population
	{"EurostatData", "EurostatRegionalPopulationData"}: 2, // Population
	{"WorldDevelopmentIndicators", ""}:                 3, // Population
	{"BLS_LAUS", "BLSSeasonallyUnadjusted"}:            0, // Unemployment Rate
	{"EurostatData", ""}:                               1, // Unemployment Rate
}

const lowestRank = 100

// Limit the concurrent channels when processing in-memory cache data.
// Without the limit, it can get very slow when process thousands of dcids
// concurrently.
const maxChannelSize = 100

// triplesToStatsVar converts a Triples cache into a StatisticalVarible object.
func triplesToStatsVar(triples *TriplesCache) (*StatisticalVariable, error) {
	// Get constraint properties.
	var propValMap map[string]string
	for _, t := range triples.Triples {
		if t.Predicate == "constraintProperties" {
			propValMap[t.ObjectID] = ""
		}
	}
	var statsVar StatisticalVariable
	// Populate the field.
	for _, t := range triples.Triples {
		object := t.ObjectID
		switch t.Predicate {
		case "typeOf":
			if object != "StatisticalVariable" {
				return nil, fmt.Errorf("%s is not a StatisticalVariable", t.SubjectID)
			}
		case "statType":
			statsVar.StatType = strings.Replace(object, "Value", "", 1)
		case "populationType":
			statsVar.PopType = object
		case "measurementMethod":
			statsVar.MeasurementMethod = object
		case "measuredProperty":
			statsVar.MeasuredProp = object
		case "measurementDenominator":
			statsVar.MeasurementDenominator = object
		case "measurementQualifier":
			statsVar.MeasurementQualifier = object
		case "scalingFactor":
			statsVar.ScalingFactor = object
		case "unit":
			statsVar.Unit = object
		default:
			if _, ok := propValMap[t.Predicate]; ok {
				statsVar.PVs[t.Predicate] = object
			}
		}
	}
	return &statsVar, nil
}

func buildKeySuffix(statsVar *StatisticalVariable) string {
	keySuffix := strings.Join([]string{
		statsVar.MeasuredProp,
		statsVar.StatType,
		statsVar.MeasurementDenominator,
		statsVar.MeasurementQualifier,
		statsVar.ScalingFactor,
		statsVar.PopType},
		"^")
	var cprops []string
	for cprop := range statsVar.PVs {
		cprops = append(cprops, cprop)
	}
	sort.Strings(cprops)
	for _, cprop := range cprops {
		keySuffix += fmt.Sprintf("^%s^%s", cprop, statsVar.PVs[cprop])
	}
	return keySuffix
}

func getObsSeries(cacheData []byte) (*pb.ObsTimeSeries, error) {
	val, err := util.UnzipAndDecode(string(cacheData))
	if err != nil {
		return nil, err
	}
	pbData := &pb.ChartStore{}
	jsonpb.UnmarshalString(string(val), pbData)
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		result := &pb.ObsTimeSeries{}
		result.Unit = x.ObsTimeSeries.GetUnit()
		result.PlaceName = x.ObsTimeSeries.GetPlaceName()
		result.IsDcAggregate = x.ObsTimeSeries.GetIsDcAggregate()
		bestScore := lowestRank
		for _, series := range x.ObsTimeSeries.SourceSeries {
			key := rankKey{series.GetImportName(), series.GetMeasurementMethod()}
			score, ok := statsRanking[key]
			if !ok {
				score = lowestRank
			}
			if score <= bestScore {
				result.Data = series.Val
				bestScore = score
			}
		}
		return result, nil
	case nil:
		return nil, fmt.Errorf("ChartStore.Val is not set")
	default:
		return nil, fmt.Errorf("ChartStore.Val has unexpected type %T", x)
	}
}

// btReadStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func btReadStats(ctx context.Context, btTable *bigtable.Table,
	rowSet bigtable.RowList) (map[string]*pb.ObsTimeSeries, error) {
	rowSetSize := len(rowSet)
	if rowSetSize == 0 {
		return nil, nil
	}
	errs, errCtx := errgroup.WithContext(ctx)
	obsSeriesChan := make(chan []*pb.ObsTimeSeries, rowSetSize)
	for i := 0; i <= rowSetSize/util.BtBatchQuerySize; i++ {
		left := i * util.BtBatchQuerySize
		right := (i + 1) * util.BtBatchQuerySize
		if right > rowSetSize {
			right = rowSetSize
		}
		var rowSetPart bigtable.RowSet
		rowSetPart = rowSet[left:right]
		errs.Go(func() error {
			var ObsSeriesSet []*pb.ObsTimeSeries
			if err := btTable.ReadRows(errCtx, rowSetPart,
				func(btRow bigtable.Row) bool {
					rowKey := btRow.Key()
					if len(btRow[util.BtFamily]) != 0 {
						dcid := btKeyToDcid(rowKey, util.BtChartDataPrefix)
						obsSeries, err := getObsSeries(btRow[util.BtFamily][0].Value)
						// Set the place dcid since it is not always available.
						obsSeries.PlaceDcid = dcid
						if err == nil {
							ObsSeriesSet = append(ObsSeriesSet, obsSeries)
						}
					}
					// Always return true otherwise the ReadRows function is cancelled.
					return true
				}); err != nil {
				return err
			}
			obsSeriesChan <- ObsSeriesSet
			return nil
		})
	}
	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(obsSeriesChan)

	result := map[string]*pb.ObsTimeSeries{}
	for obsSeriesSet := range obsSeriesChan {
		for _, obsSeries := range obsSeriesSet {
			result[obsSeries.PlaceDcid] = obsSeries
		}
	}
	return result, nil
}

// Read stats data from the in-memory branch cache.
func memReadStats(rowList bigtable.RowList, cache *Cache) (
	map[string]*pb.ObsTimeSeries, error) {
	obsSeriesChan := make(chan *pb.ObsTimeSeries, maxChannelSize)
	errs := errgroup.Group{}
	for _, rowKey := range rowList {
		rowKey := rowKey
		errs.Go(func() error {
			if data, ok := cache.Read(rowKey); ok {
				dcid := btKeyToDcid(rowKey, util.BtChartDataPrefix)
				obsSeries, err := getObsSeries(data)
				if err != nil {
					return nil
				}
				// Set the place dcid since it is not always available.
				obsSeries.PlaceDcid = dcid
				obsSeriesChan <- obsSeries
				return nil
			}
			return nil
		})
	}
	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(obsSeriesChan)
	var result map[string]*pb.ObsTimeSeries
	for obsSeries := range obsSeriesChan {
		result[obsSeries.PlaceDcid] = obsSeries
	}
	return result, nil
}

func (s *store) GetStats(ctx context.Context, in *pb.GetStatsRequest,
	out *pb.GetStatsResponse) error {
	// Read triples for stats var.
	triples, err := s.ReadTriples(ctx, in.GetStatsVar())
	if err != nil {
		return err
	}
	// Get the StatisticalVariable
	statsVar, err := triplesToStatsVar(triples)
	if err != nil {
		return err
	}
	// Construct BigTable row keys.
	keySuffix := buildKeySuffix(statsVar)
	rowList := bigtable.RowList{}
	for _, dcid := range in.GetPlace() {
		rowKey := fmt.Sprintf("%s%s^%s", util.BtChartDataPrefix, dcid, keySuffix)
		rowList = append(rowList, rowKey)
	}
	// Map from place dcid to compressed cache data.
	var result map[string]*pb.ObsTimeSeries
	// Read data from branch in-momery cache first.
	if in.GetOption().GetCacheChoice() != pb.Option_BASE_CACHE_ONLY {
		result, err = memReadStats(rowList, s.cache)
	}
	if err != nil {
		return err
	}
	// Read data from base cache if no branch cache data is found.
	// This is valid since branch cache is a superset of base cache.
	if len(result) == 0 {
		result, err = btReadStats(ctx, s.btTable, rowList)
	}
	if err != nil {
		return err
	}

	// Fill missing place data and result result
	for _, dcid := range in.GetPlace() {
		if _, ok := result[dcid]; !ok {
			result[dcid] = nil
		}
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)
	return nil
}
