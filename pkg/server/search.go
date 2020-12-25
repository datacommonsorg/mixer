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

package server

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"

	pb "github.com/datacommonsorg/mixer/pkg/proto"

	"google.golang.org/api/iterator"
)

// Search implements API for Mixer.Search.
func (s *Server) Search(
	ctx context.Context, in *pb.SearchRequest) (*pb.SearchResponse, error) {
	result := map[string]*pb.SearchResultSection{}
	tokens := strings.Split(strings.ToLower(in.GetQuery()), " ")
	qStr := fmt.Sprintf(
		"SELECT id, type, extended_name FROM `%s`.Instance "+
			"WHERE type != \"CensusTract\" and type != \"PowerPlant\""+
			" and type != \"PowerPlantUnit\""+
			" and type != \"BiologicalSpecimen\"", s.metadata.Bq)
	for _, token := range tokens {
		qStr += fmt.Sprintf(
			` AND REGEXP_CONTAINS(LOWER(extended_name), r"\b%s\b")`, token)
	}
	if in.GetMaxResults() > 0 {
		qStr += fmt.Sprintf(" LIMIT %d", in.GetMaxResults())
	}
	q := s.bqClient.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		dcid := row[0].(string)
		typeName := row[1].(string)
		name := row[2].(string)

		if _, ok := result[typeName]; !ok {
			result[typeName] = &pb.SearchResultSection{TypeName: typeName}

		}
		section := result[typeName]
		section.Entity = append(
			section.Entity,
			&pb.SearchEntityResult{Dcid: dcid, Name: name},
		)
	}
	out := pb.SearchResponse{}
	for _, v := range result {
		out.Section = append(out.Section, v)
	}

	return &out, nil
}
