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
	"log"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"github.com/golang/protobuf/jsonpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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
	rankKey{"CensusPEP", "CensusPEPSurvey"}:                   0, // Population
	rankKey{"CensusACS5YearSurvey", "CensusACS5yrSurvey"}:     1, // Population
	rankKey{"EurostatData", "EurostatRegionalPopulationData"}: 2, // Population
	rankKey{"WorldDevelopmentIndicators", ""}:                 3, // Population
	rankKey{"BLS_LAUS", "BLSSeasonallyUnadjusted"}:            0, // Unemployment Rate
	rankKey{"EurostatData", ""}:                               1, // Unemployment Rate
}

const lowestRank = 100

// PopObs represents a pair of population and observation node.
type PopObs struct {
	PopulationID     string `json:"dcid,omitempty"`
	ObservationValue string `json:"observation,omitempty"`
}

func (s *store) GetPopObs(ctx context.Context, in *pb.GetPopObsRequest,
	out *pb.GetPopObsResponse) error {
	dcid := in.GetDcid()
	key := util.BtPopObsPrefix + dcid

	var baseData, branchData pb.PopObsPlace
	var baseString, branchString string
	var hasBaseData, hasBranchData bool
	out.Payload, _ = util.ZipAndEncode("{}")

	btRow, err := s.btTable.ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}

	hasBaseData = len(btRow[util.BtFamily]) > 0
	if hasBaseData {
		baseString = string(btRow[util.BtFamily][0].Value)
	}
	if in.GetOption().GetCacheChoice() == pb.Option_BASE_CACHE_ONLY {
		hasBranchData = false
	} else {
		branchString, hasBranchData = s.cache.Read(key)
	}

	if !hasBaseData && !hasBranchData {
		return nil
	} else if !hasBaseData {
		out.Payload = branchString
		return nil
	} else if !hasBranchData {
		out.Payload = baseString
		return nil
	} else {
		if tmp, err := util.UnzipAndDecode(baseString); err == nil {
			jsonpb.UnmarshalString(string(tmp), &baseData)
		}
		if tmp, err := util.UnzipAndDecode(branchString); err == nil {
			jsonpb.UnmarshalString(string(tmp), &branchData)
		}
		if baseData.Populations == nil {
			baseData.Populations = map[string]*pb.PopObsPop{}
		}
		for k, v := range branchData.Populations {
			baseData.Populations[k] = v
		}
		resStr, err := (&jsonpb.Marshaler{}).MarshalToString(&baseData)
		if err != nil {
			return err
		}
		out.Payload, err = util.ZipAndEncode(resStr)
		return err
	}
}

func (s *store) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest,
	out *pb.GetPlaceObsResponse) error {
	key := fmt.Sprintf("%s^%s^%s", in.GetPlaceType(), in.GetObservationDate(),
		in.GetPopulationType())
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "^" + p + "^" + v
		})
	}
	key = fmt.Sprintf("%s%s", util.BtPlaceObsPrefix, key)

	// TODO(boxu): abstract out the common logic for handling cache merging.
	var baseData, branchData pb.PopObsCollection
	var baseString, branchString string
	var hasBaseData, hasBranchData bool
	out.Payload, _ = util.ZipAndEncode("{}")

	btRow, err := s.btTable.ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}

	hasBaseData = len(btRow[util.BtFamily]) > 0
	if hasBaseData {
		baseString = string(btRow[util.BtFamily][0].Value)
	}
	if in.GetOption().GetCacheChoice() == pb.Option_BASE_CACHE_ONLY {
		hasBranchData = false
	} else {
		branchString, hasBranchData = s.cache.Read(key)
	}

	if !hasBaseData && !hasBranchData {
		return nil
	} else if !hasBaseData {
		out.Payload = branchString
		return nil
	} else if !hasBranchData {
		out.Payload = baseString
		return nil
	} else {
		if tmp, err := util.UnzipAndDecode(baseString); err == nil {
			jsonpb.UnmarshalString(string(tmp), &baseData)
		}
		if tmp, err := util.UnzipAndDecode(branchString); err == nil {
			jsonpb.UnmarshalString(string(tmp), &branchData)
		}
		dataMap := map[string]*pb.PopObsPlace{}
		for _, data := range baseData.Places {
			dataMap[data.Place] = data
		}
		for _, data := range branchData.Places {
			dataMap[data.Place] = data
		}
		res := pb.PopObsCollection{}
		for _, v := range dataMap {
			res.Places = append(res.Places, v)
		}
		resStr, err := (&jsonpb.Marshaler{}).MarshalToString(&res)
		if err != nil {
			return err
		}
		out.Payload, err = util.ZipAndEncode(resStr)
		return err
	}
}

func (s *store) GetObsSeries(ctx context.Context, in *pb.GetObsSeriesRequest,
	out *pb.GetObsSeriesResponse) error {
	key := fmt.Sprintf("%s^%s", in.GetPlace(), in.GetPopulationType())
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "^" + p + "^" + v
		})
	}
	btPrefix := fmt.Sprintf("%s%s", util.BtObsSeriesPrefix, key)

	// Query for the prefix.
	btRow, err := s.btTable.ReadRow(ctx, btPrefix)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 {
		out.Payload = string(btRow[util.BtFamily][0].Value)
	}
	return nil
}

func (s *store) GetPopCategory(ctx context.Context,
	in *pb.GetPopCategoryRequest, out *pb.GetPopCategoryResponse) error {
	btRow, err := s.btTable.ReadRow(ctx, util.BtPopCategoryPrefix+in.GetPlaceType())
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 {
		out.Payload = string(btRow[util.BtFamily][0].Value)
	}

	return nil
}

// PlacePopInfo contains basic info for a place and a population.
type PlacePopInfo struct {
	PlaceID      string `json:"dcid,omitempty"`
	PopulationID string `json:"population,omitempty"`
}

func (s *store) GetPopulations(ctx context.Context, in *pb.GetPopulationsRequest,
	out *pb.GetPopulationsResponse) error {
	if err := s.btGetPopulations(ctx, in, out); err != nil {
		return err
	}
	return nil
}

func (s *store) bqGetPopulations(ctx context.Context, in *pb.GetPopulationsRequest,
	out *pb.GetPopulationsResponse) error {
	collection := []*PlacePopInfo{}

	// Construct the query string.
	numConstraints := len(in.GetPvs())
	qStr := fmt.Sprintf("SELECT p.id, p.place_key "+
		"FROM `%s.StatisticalPopulation` AS p "+
		"WHERE p.place_key IN (%s) "+
		"AND p.population_type = \"%s\" "+
		"AND p.num_constraints = %d",
		s.bqDb, util.StringList(in.GetDcids()), in.GetPopulationType(), numConstraints)
	if numConstraints > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			qStr += fmt.Sprintf(" AND p.p%d = \"%s\" AND p.v%d = \"%s\"",
				i+1, p, i+1, v)
		})
	}

	// Log the query string.
	log.Printf("GetPopulations: Sending query \"%s\"", qStr)

	// Issue the query to BQ.
	q := s.bqClient.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return nil
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		collection = append(collection, &PlacePopInfo{
			PlaceID:      row[1].(string),
			PopulationID: row[0].(string),
		})
	}

	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

func (s *store) btGetPopulations(ctx context.Context, in *pb.GetPopulationsRequest,
	out *pb.GetPopulationsResponse) error {
	dcids := in.GetDcids()

	// Create the cache key suffix
	keySuffix := "^" + in.GetPopulationType()
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			keySuffix += ("^" + p + "^" + v)
		})
	}

	// Generate the list of all keys to query cache for
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		btKey := util.BtPopPrefix + dcid + keySuffix
		rowList = append(rowList, btKey)
	}

	// Query the cache
	collection := []*PlacePopInfo{}
	dcidStore := map[string]struct{}{}
	if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			rowKey := btRow.Key()
			parts := strings.Split(rowKey, "^")
			dcid := strings.TrimPrefix(parts[0], util.BtPopPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				popIDRaw, err := util.UnzipAndDecode(string(btRow[util.BtFamily][0].Value))
				if err != nil {
					return err
				}
				popIDFmt := string(popIDRaw)
				if len(popIDFmt) > 0 {
					collection = append(collection, &PlacePopInfo{
						PlaceID:      dcid,
						PopulationID: popIDFmt,
					})
					dcidStore[dcid] = struct{}{}
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

func (s *store) GetObservations(ctx context.Context, in *pb.GetObservationsRequest,
	out *pb.GetObservationsResponse) error {
	if err := s.btGetObservations(ctx, in, out); err != nil {
		return err
	}
	return nil
}

func (s *store) bqGetObservations(ctx context.Context, in *pb.GetObservationsRequest,
	out *pb.GetObservationsResponse) error {
	// Construct the query string.
	qStr := fmt.Sprintf(
		"SELECT id, %s FROM `%s.Observation` "+
			"WHERE observed_node_key IN (%s) "+
			"AND observation_date = \"%s\" "+
			"AND measured_prop = \"%s\"",
		util.CamelToSnake(in.GetStatsType()),
		s.bqDb,
		util.StringList(in.GetDcids()),
		in.GetObservationDate(),
		in.GetMeasuredProperty(),
	)

	// Add optional parameters for an observations
	if in.GetObservationPeriod() != "" {
		qStr += fmt.Sprintf(
			"AND observation_period = \"%s\" ",
			in.GetObservationPeriod(),
		)
	} else {
		qStr += " AND observation_period is NULL"
	}
	if in.GetMeasurementMethod() != "" {
		qStr += fmt.Sprintf(
			"AND measurement_method = \"%s\" ",
			in.GetMeasurementMethod(),
		)
	} else {
		qStr += " AND measurement_method is NULL"
	}

	// Log the query string.
	log.Printf("GetObservations: Sending query \"%s\"", qStr)

	// Execute the query and collect the response.
	q := s.bqClient.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	collection := []PopObs{}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		var id string
		var m float64
		for i, cell := range row {
			if cell == nil {
				continue
			}
			if i == 0 {
				id = cell.(string)
			} else if i == 1 {
				m = cell.(float64)
			}
		}
		collection = append(collection, PopObs{id, strconv.FormatFloat(m, 'f', 6, 64)})
	}

	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

func (s *store) btGetObservations(ctx context.Context, in *pb.GetObservationsRequest,
	out *pb.GetObservationsResponse) error {
	dcids := in.GetDcids()

	// Construct the list of cache keys to query.
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		btKey := fmt.Sprintf("%s%s^%s^%s^%s^%s^%s^^^",
			util.BtObsPrefix, dcid, in.GetMeasuredProperty(),
			util.SnakeToCamel(in.GetStatsType()), in.GetObservationDate(),
			in.GetObservationPeriod(), in.GetMeasurementMethod())
		rowList = append(rowList, btKey)
	}

	// Query the cache for all keys.
	collection := []*PopObs{}
	dcidStore := map[string]struct{}{}
	if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			rowKey := btRow.Key()
			parts := strings.Split(rowKey, "^")
			dcid := strings.TrimPrefix(parts[0], util.BtObsPrefix)

			// Add the results of the query.
			if len(btRow[util.BtFamily]) > 0 {
				valRaw, err := util.UnzipAndDecode(string(btRow[util.BtFamily][0].Value))
				if err != nil {
					return err
				}

				valFmt := string(valRaw)
				if len(valFmt) > 0 {
					collection = append(collection, &PopObs{
						PopulationID:     dcid,
						ObservationValue: valFmt,
					})
					dcidStore[dcid] = struct{}{}
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

// iterateSortPVs iterates a list of PVs and performs actions on them.
func iterateSortPVs(pvs []*pb.PropertyValue, action func(i int, p, v string)) {
	pvMap := map[string]string{}
	pList := []string{}
	for _, pv := range pvs {
		pvMap[pv.GetProperty()] = pv.GetValue()
		pList = append(pList, pv.GetProperty())
	}
	sort.Strings(pList)
	for i, p := range pList {
		action(i, p, pvMap[p])
	}
}

func keyToDcid(key, prefix string) string {
	parts := strings.Split(key, "^")
	return strings.TrimPrefix(parts[0], prefix)
}

type dcidObs struct {
	dcid      string
	obsSeries *pb.ObsTimeSeries
}

func getObsSeries(
	dcid string,
	cacheValue string,
	statsVar *pb.StatisticalVariable,
) (*pb.ObsTimeSeries, error) {
	val, err := util.UnzipAndDecode(cacheValue)
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

func (s *store) GetStats(ctx context.Context, in *pb.GetStatsRequest,
	out *pb.GetStatsResponse) error {
	statsVarKey := fmt.Sprintf("%s%s", util.BtTriplesPrefix, in.GetStatsVar())
	// Query for stats var.
	btRow, err := s.btTable.ReadRow(ctx, statsVarKey)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) == 0 {
		return nil
	}

	btRawValue := string(btRow[util.BtFamily][0].Value)
	btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
	if err != nil {
		return err
	}
	var btTriples TriplesCache
	json.Unmarshal(btJSONRaw, &btTriples)

	var statsVar pb.StatisticalVariable
	// TODO(boxu): Remove when data is fixed.
	allP := map[string]struct{}{}
	var pvs []*pb.PropertyValue
	for _, t := range btTriples.Triples {
		if t.Predicate == "typeOf" {
			if t.ObjectID != "StatisticalVariable" {
				return fmt.Errorf("%s is not a StatisticalVariable", in.GetStatsVar())
			}
		} else if t.Predicate == "statType" {
			statsVar.StatType = strings.Replace(t.ObjectID, "Value", "", 1)
		} else if t.Predicate == "provenance" {
			continue
		} else if t.Predicate == "name" {
			continue
		} else if t.Predicate == "censusACSTableId" {
			continue
		} else if t.Predicate == "constraintProperties" {
			continue
		} else if t.Predicate == "populationType" {
			statsVar.PopType = t.ObjectID
		} else if t.Predicate == "measurementMethod" {
			statsVar.MeasurementMethod = t.ObjectID
		} else if t.Predicate == "measuredProperty" {
			statsVar.MeasuredProp = t.ObjectID
		} else if t.Predicate == "measurementDenominator" {
			statsVar.MeasurementDenominator = t.ObjectID
		} else if t.Predicate == "measurementQualifier" {
			statsVar.MeasurementQualifier = t.ObjectID
		} else if t.Predicate == "scalingFactor" {
			statsVar.ScalingFactor = t.ObjectID
		} else if t.Predicate == "unit" {
			statsVar.Unit = t.ObjectID
		} else {
			if _, ok := allP[t.Predicate]; !ok {
				// Do not use the pvs in pb.StatisticalVariable. Instead use the
				// pb.PropertyValue array to use the sorting function.
				pvs = append(pvs, &pb.PropertyValue{Property: t.Predicate, Value: t.ObjectID})
				allP[t.Predicate] = struct{}{}
			}
		}
	}

	keySuffix := strings.Join([]string{
		statsVar.MeasuredProp,
		statsVar.StatType,
		statsVar.MeasurementDenominator,
		statsVar.MeasurementQualifier,
		statsVar.ScalingFactor,
		statsVar.PopType},
		"^")

	if len(pvs) > 0 {
		iterateSortPVs(pvs, func(i int, p, v string) {
			keySuffix += "^" + p + "^" + v
		})
	}
	rowList := bigtable.RowList{}
	for _, dcid := range in.GetPlace() {
		rowList = append(rowList, fmt.Sprintf("%s%s^%s", util.BtChartDataPrefix, dcid, keySuffix))
	}

	dcidToRaw := map[string]string{}

	if in.GetOption().GetCacheChoice() != pb.Option_BASE_CACHE_ONLY {
		for _, rowKey := range rowList {
			rowKey := rowKey
			if branchString, ok := s.cache.Read(rowKey); ok {
				dcid := keyToDcid(rowKey, util.BtChartDataPrefix)
				dcidToRaw[dcid] = branchString
			}
		}
	}

	// Read result from base cache if no branch cache data found.
	// This is valid since branch cache is a superset of base cache.
	if len(dcidToRaw) == 0 {
		if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
			func(btRow bigtable.Row) error {
				rowKey := btRow.Key()
				if len(btRow[util.BtFamily]) > 0 {
					dcid := keyToDcid(rowKey, util.BtChartDataPrefix)
					dcidToRaw[dcid] = string(btRow[util.BtFamily][0].Value)
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
			return err
		}
	}

	result := map[string]*pb.ObsTimeSeries{}
	dcidObsChan := make(chan *dcidObs, len(dcidToRaw))
	errs, _ := errgroup.WithContext(ctx)
	for dcid, data := range dcidToRaw {
		dcid := dcid
		data := data
		errs.Go(func() error {
			obsSeries, err := getObsSeries(
				dcid,
				string(data),
				&statsVar,
			)
			if err != nil {
				return err
			}
			dcidObsChan <- &dcidObs{dcid, obsSeries}
			return nil
		})
	}
	err = errs.Wait()
	if err != nil {
		return err
	}
	close(dcidObsChan)

	for item := range dcidObsChan {
		result[item.dcid] = item.obsSeries
	}

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
