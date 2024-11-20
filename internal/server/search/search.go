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

package search

import (
	"context"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigquery"

	pb "github.com/datacommonsorg/mixer/internal/proto"

	"google.golang.org/api/iterator"
)

const MaxSearchLimit = 100

// Search implements API for Mixer.Search.
func Search(
	ctx context.Context,
	in *pb.SearchRequest,
	bqClient *bigquery.Client,
	tableName string,
) (*pb.SearchResponse, error) {
	tokens := strings.Split(strings.ToLower(in.GetQuery()), " ")
	if len(tokens) == 1 && tokens[0] == "" {
		return nil, fmt.Errorf("query not specified")
	}

	var queryParams []bigquery.QueryParameter

	qStr := fmt.Sprintf(
		"SELECT id, type, extended_name FROM `%s`.Instance "+
			"WHERE type != \"CensusTract\" and type != \"PowerPlant\""+
			" and type != \"PowerPlantUnit\""+
			" and type != \"BiologicalSpecimen\"", tableName)

	for i, token := range tokens {
		paramName := fmt.Sprintf("token%d", i)
		qStr += fmt.Sprintf(" AND REGEXP_CONTAINS(LOWER(extended_name), @%s)", paramName)
		queryParams = append(queryParams, bigquery.QueryParameter{Name: paramName, Value: fmt.Sprintf(`\b%s\b`, token)})
	}

	maxResults := in.GetMaxResults()
	if maxResults <= 0 {
		maxResults = MaxSearchLimit
	}
	limit := int(math.Min(MaxSearchLimit, float64(maxResults)))
	qStr += " LIMIT @limit"
	queryParams = append(queryParams, bigquery.QueryParameter{Name: "limit", Value: limit})

	q := bqClient.Query(qStr)
	q.Parameters = queryParams
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	result := map[string]*pb.SearchResultSection{}
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
