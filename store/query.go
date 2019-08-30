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

	"cloud.google.com/go/bigquery"
	"github.com/datacommonsorg/mixer/base"
	"github.com/datacommonsorg/mixer/sparql"
	"github.com/datacommonsorg/mixer/translator"

	pb "github.com/datacommonsorg/mixer/proto"

	"google.golang.org/api/iterator"
)

func (s *store) Query(
	ctx context.Context, in *pb.QueryRequest, out *pb.QueryResponse) error {
	opts := &base.QueryOptions{}

	var (
		nodes   []base.Node
		queries []*base.Query
		err     error
	)

	nodes, queries, opts, err = sparql.ParseQuery(in.GetSparql())
	if err != nil {
		return err
	}

	translation, err := translator.Translate(s.bqMapping, nodes, queries, s.subTypeMap, opts)
	if err != nil {
		return err
	}

	log.Printf("translated SQL query for Bigquery:%v\n", translation.SQL)

	for _, node := range translation.Nodes {
		out.Header = append(out.Header, node.Alias)
	}
	out.Rows = []*pb.QueryResponseRow{}
	n := len(out.Header)

	q := s.bqClient.Query(translation.SQL)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	for {
		responseRow := pb.QueryResponseRow{}
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		for i, cell := range row {
			var str string
			if cell != nil {
				if x, ok := cell.(int64); ok {
					str = fmt.Sprintf("%v", x)
				}
				if x, ok := cell.(float64); ok {
					str = fmt.Sprintf("%v", x)
				}
				if x, ok := cell.(string); ok {
					str = x
				}
			}
			if i < n {
				responseRow.Cells = append(responseRow.Cells, &pb.QueryResponseCell{Value: str})
			} else {
				// Add provenance to corresponding cells.
				if idx, ok := translation.Prov[i]; ok {
					for _, j := range idx {
						responseRow.Cells[j].ProvenanceId = str
					}
				}
			}
		}
		out.Rows = append(out.Rows, &responseRow)
	}
	return nil
}
