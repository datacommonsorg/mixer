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
	"google.golang.org/api/iterator"
)

// PopObs represents a pair of population and observation node.
type PopObs struct {
	PopulationID     string `json:"dcid,omitempty"`
	ObservationValue string `json:"observation,omitempty"`
}

func (s *store) GetPopObs(ctx context.Context, in *pb.GetPopObsRequest,
	out *pb.GetPopObsResponse) error {
	dcid := in.GetDcid()
	btPrefix := fmt.Sprintf("%s%s", util.BtPopObsPrefix, dcid)

	// Query for the prefix
	btRow, err := s.btTable.ReadRow(ctx, btPrefix)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 && btRow[util.BtFamily][0].Row == btPrefix {
		out.Payload = string(btRow[util.BtFamily][0].Value)
	}
	return nil
}

func (s *store) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest,
	out *pb.GetPlaceObsResponse) error {
	key := fmt.Sprintf("%s-%s-%s", in.GetPlaceType(), in.GetObservationDate(),
		in.GetPopulationType())
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "-" + p + "-" + v
		})
	}
	btPrefix := fmt.Sprintf("%s%s", util.BtPlaceObsPrefix, key)

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

func (s *store) GetObsSeries(ctx context.Context, in *pb.GetObsSeriesRequest,
	out *pb.GetObsSeriesResponse) error {
	key := fmt.Sprintf("%s-%s", in.GetPlace(), in.GetPopulationType())
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "-" + p + "-" + v
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
	keySuffix := "-" + in.GetPopulationType()
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			keySuffix += ("-" + p + "-" + v)
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
			parts := strings.Split(rowKey, "-")
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
		btKey := fmt.Sprintf("%s%s-%s-%s-%s-%s-%s",
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
			parts := strings.Split(rowKey, "-")
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
