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
	"fmt"
	"log"

	"encoding/json"

	"cloud.google.com/go/bigquery"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/iterator"
)

// ----------------------------- BQ CLIENT METHODS -----------------------------

func (s *store) GetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error {
	// By default, return empty list.
	out.Payload = "[]"

	// If no DCIDs given, return empty result
	parentIds := in.GetDcids()
	if len(parentIds) == 0 {
		return nil
	}

	// Get typing information from the first node.
	nodeInfo, err := getNodeInfo(ctx, s.bqClient, s.bqDb, parentIds[:1])
	if err != nil {
		return err
	}
	n, ok := nodeInfo[parentIds[0]]
	if !ok {
		return nil
	}
	parentType := n.Types[0]
	interTypes, ok := s.containedIn[util.TypePair{Child: in.GetPlaceType(), Parent: parentType}]
	if !ok {
		return nil
	}

	// SELECT t1.subject_id FROM `google.com:datcom-store-dev.dc_v3_internal.Triple` as t1
	// JOIN `google.com:datcom-store-dev.dc_v3_internal.Instance` as i
	// ON t1.subject_id = i.id AND i.type = "City"
	// JOIN `google.com:datcom-store-dev.dc_v3_internal.Triple` as t2 ON t1.object_id = t2.subject_id
	// WHERE t2.predicate = "containedInPlace" and t2.object_id = "geoId/06"

	// Build the query string.
	qStr := fmt.Sprintf(
		"SELECT t%d.object_id, t0.subject_id "+
			"FROM `%s.Triple` as t0 "+
			"JOIN `%s.Instance` as i ON t0.subject_id = i.id AND i.type = '%s' ",
		len(interTypes), s.bqDb, s.bqDb, in.GetPlaceType())
	for i := range interTypes {
		qStr += fmt.Sprintf(
			"JOIN `%s.Triple` as t%d ON t%d.object_id = t%d.subject_id AND t%d.predicate = 'containedInPlace'",
			s.bqDb, i+1, i, i+1, i+1)
	}
	qStr += fmt.Sprintf("WHERE t%d.object_id IN (%s)", len(interTypes), util.StringList(parentIds))
	log.Printf("GetPlacesIn: %v\n", qStr)

	// Issue the query and build the resulting json.
	jsonRes := make([]map[string]string, 0)
	query := s.bqClient.Query(qStr)
	it, err := query.Read(ctx)
	if err != nil {
		return err
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
		jsonRes = append(jsonRes, map[string]string{
			"dcid":  row[0].(string),
			"place": row[1].(string),
		})
	}

	jsonRaw, err := json.Marshal(jsonRes)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

func (s *store) GetPlaceKML(ctx context.Context,
	in *pb.GetPlaceKMLRequest, out *pb.GetPlaceKMLResponse) error {
	btTable := s.btClient.Open(util.BtTable)
	btPrefix := util.BtPlaceKMLPrefix + in.GetDcid()
	btRow, err := btTable.ReadRow(ctx, btPrefix)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 && btRow[util.BtFamily][0].Row == btPrefix {
		out.Payload = string(btRow[util.BtFamily][0].Value)
	}
	return nil
}
