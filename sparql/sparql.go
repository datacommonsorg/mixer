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

// Package sparql parses Sparql query for translation.
package sparql

import (
	"strings"

	"github.com/datacommonsorg/mixer/base"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ParseQuery parses a sparql query into list of nodes and list of query statements.
func ParseQuery(queryString string) ([]base.Node, []*base.Query, *base.QueryOptions, error) {
	queryTree, err := NewParser(strings.NewReader(queryString)).Parse()
	if err != nil {
		return nil, nil, nil, status.Errorf(
			codes.InvalidArgument, "Invalid sparql query string\n%s", queryString)
	}
	opts := base.QueryOptions{Limit: queryTree.L, Distinct: queryTree.S.Distinct}

	nodes := []base.Node{}
	for _, v := range queryTree.S.Variable {
		nodes = append(nodes, base.NewNode(v))
	}

	queries := []*base.Query{}
	for _, t := range queryTree.W.Triples {
		var query *base.Query
		if len(t.Objs) == 1 {
			obj := t.Objs[0]
			if strings.HasPrefix(obj, "?") {
				query = base.NewQuery(t.Pred, t.Sub, base.NewNode(obj))
			} else {
				query = base.NewQuery(t.Pred, t.Sub, obj)
			}
		} else {
			query = base.NewQuery(t.Pred, t.Sub, t.Objs)
		}
		queries = append(queries, query)
	}
	if queryTree.O != nil {
		opts.Orderby = queryTree.O.Variable
		opts.ASC = queryTree.O.ASC
	}
	return nodes, queries, &opts, nil
}
