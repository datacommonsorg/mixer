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

func (s *store) GetPopObs(ctx context.Context, in *pb.GetPopObsRequest, out *pb.GetPopObsResponse) error {
	dcid := in.GetDcid()
	btPrefix := fmt.Sprintf("%s%s", util.BtPopObsPrefix, dcid)
	btTable := s.btClient.Open(util.BtTable)

	// Query for the prefix
	btRow, err := btTable.ReadRow(ctx, btPrefix)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 && btRow[util.BtFamily][0].Row == btPrefix {
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
	// By default, return empty list.
	out.Payload = "[]"
	if err := s.btGetPopulations(ctx, in, out); err != nil {
		return err
	}
	if out.GetPayload() == "[]" {
		if err := s.bqGetPopulations(ctx, in, out); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) bqGetPopulations(ctx context.Context, in *pb.GetPopulationsRequest,
	out *pb.GetPopulationsResponse) error {
	collection := []*PlacePopInfo{}

	numConstraints := len(in.GetPvs())
	qStr := fmt.Sprintf("SELECT p.id, p.place_key "+
		"FROM `%s.StatisticalPopulation` AS p "+
		"WHERE p.place_key IN (%s) "+
		"AND p.num_constraints = %d",
		s.bqDb, util.StringList(in.GetDcids()), numConstraints)

	if in.GetPopulationType() != "" {
		qStr += fmt.Sprintf(" AND p.population_type = \"%s\"", in.GetPopulationType())
	}

	if numConstraints > 0 {
		util.IterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			qStr += fmt.Sprintf(" AND p.p%d = \"%s\" AND p.v%d = \"%s\"",
				i+1, p, i+1, v)
		})
	}

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

	keySuffix := "-" + in.GetPopulationType()
	if len(in.GetPvs()) > 0 {
		util.IterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			keySuffix += ("-" + p + "-" + v)
		})
	}

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, util.BtPopPrefix+dcid+keySuffix)
	}

	collection := []*PlacePopInfo{}
	dcidStore := map[string]struct{}{}
	if err := util.BigTableReadRowsParallel(ctx, s.btClient, rowList,
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
				collection = append(collection, &PlacePopInfo{
					PlaceID:      dcid,
					PopulationID: string(popIDRaw),
				})
				dcidStore[dcid] = struct{}{}
			}
			return nil
		}); err != nil {
		return err
	}

	// Iterate through all dcids to make sure they are present in the final result.
	for _, dcid := range dcids {
		if _, ok := dcidStore[dcid]; !ok {
			collection = append(collection, &PlacePopInfo{PlaceID: dcid})
		}
	}

	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

func (s *store) GetObservations(ctx context.Context, in *pb.GetObservationsRequest,
	out *pb.GetObservationsResponse) error {
	// By default, return empty list.
	out.Payload = "[]"
	if err := s.btGetObservations(ctx, in, out); err != nil {
		return err
	}
	if out.GetPayload() == "[]" {
		if err := s.bqGetObservations(ctx, in, out); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) bqGetObservations(ctx context.Context, in *pb.GetObservationsRequest,
	out *pb.GetObservationsResponse) error {
	qStr := fmt.Sprintf(
		"SELECT id, %s FROM `%s.Observation` WHERE observed_node_key IN (%s) AND observation_date = \"%s\" AND measured_prop = \"%s\" ",
		util.CamelToSnake(in.GetStatsType()),
		s.bqDb,
		util.StringList(in.GetDcids()),
		in.GetObservationDate(),
		in.GetMeasuredProperty(),
	)
	if in.GetObservationPeriod() != "" {
		qStr += fmt.Sprintf("AND observation_period = \"%s\" ", in.GetObservationPeriod())
	}
	if in.GetMeasurementMethod() != "" {
		qStr += fmt.Sprintf("AND measurement_method = \"%s\" ", in.GetMeasurementMethod())
	}
	log.Printf("Query: %v\n", qStr)
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

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList,
			fmt.Sprintf("%s%s-%s-%s-%s-%s-%s", util.BtObsPrefix, dcid, in.GetMeasuredProperty(),
				util.SnakeToCamel(in.GetStatsType()), in.GetObservationDate(),
				in.GetObservationPeriod(), in.GetMeasurementMethod()))
	}

	collection := []*PopObs{}
	dcidStore := map[string]struct{}{}
	if err := util.BigTableReadRowsParallel(ctx, s.btClient, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			rowKey := btRow.Key()
			parts := strings.Split(rowKey, "-")
			dcid := strings.TrimPrefix(parts[0], util.BtObsPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				valRaw, err := util.UnzipAndDecode(string(btRow[util.BtFamily][0].Value))
				if err != nil {
					return err
				}
				collection = append(collection, &PopObs{
					PopulationID:     dcid,
					ObservationValue: string(valRaw),
				})
				dcidStore[dcid] = struct{}{}
			}
			return nil
		}); err != nil {
		return err
	}

	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}
