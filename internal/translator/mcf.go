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

package translator

import (
	"strings"

	"github.com/datacommonsorg/mixer/internal/base"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// split datalog query by comma but skip comma in double quotes.
func split(str string, sep rune) ([]string, error) {
	results := []string{}
	last := -1
	inQuotes := false
	for curr, char := range str {
		if char == sep && !inQuotes {
			results = append(results, str[last+1:curr])
			last = curr
		} else if char == '"' && str[last] != '\\' {
			inQuotes = !inQuotes
		}
		if curr == len(str)-1 && curr-last > 0 {
			results = append(results, str[last+1:])
		}
	}
	if inQuotes {
		return nil, status.Error(codes.InvalidArgument, "Unpaired quotes")
	}
	for i, v := range results {
		results[i] = strings.TrimSpace(v)
	}
	return results, nil
}

// ParseQuery parses a datalog query into list of nodes and list of query statements.
func ParseQuery(queryString string) ([]base.Node, []*base.Query, error) {
	statements, err := split(strings.TrimSpace(queryString), ',')

	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "Found unpaired quotes for query: %s", queryString)
	}

	if len(statements) < 2 {
		return nil, nil, status.Errorf(codes.InvalidArgument, "Query separated by comma: %s", queryString)
	}

	sVars := strings.Fields(statements[0])
	if strings.ToUpper(sVars[0]) != "SELECT" {
		return nil, nil, status.Errorf(codes.InvalidArgument, "Query does starts with SELECT: %s", queryString)
	}

	nodes := []base.Node{}
	for _, alias := range sVars[1:] {
		nodes = append(nodes, base.NewNode(alias))
	}

	queries := []*base.Query{}
	for _, statement := range statements[1:] {
		tmp, err := split(strings.TrimSpace(statement), ' ')
		if err != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "Found unpaired quotes")
		}
		terms := []string{}
		for _, term := range tmp {
			if term != "" {
				terms = append(terms, term)
			}
		}

		if len(terms) < 3 {
			return nil, nil, status.Errorf(codes.InvalidArgument, "Query terms length < 3: %s", terms)
		}
		var query *base.Query
		if strings.HasPrefix(terms[2], "?") {
			query = base.NewQuery(terms[0], terms[1], base.NewNode(terms[2]))
		} else {
			if len(terms) == 3 {
				query = base.NewQuery(terms[0], terms[1], terms[2])
			} else {
				query = base.NewQuery(terms[0], terms[1], terms[2:])

			}
		}
		queries = append(queries, query)
	}
	return nodes, queries, nil
}

// ParseMapping parses schema mapping mcf into a list of Mapping struct.
func ParseMapping(mcf, database string) ([]*base.Mapping, error) {
	lines := strings.Split(mcf, "\n")
	mappings := []*base.Mapping{}
	var sub string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) < 2 {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid schema mapping mcf:\n%s", mcf)
		}
		head := strings.TrimSpace(parts[0])
		body := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(parts[1]), `"`), `"`)

		if head == "Node" {
			sub = body
		} else {
			if sub == "" {
				return nil, status.Error(codes.InvalidArgument, "Missing Node identifier")
			}
			m, err := base.NewMapping(head, sub, body, database)
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, m)
		}
	}
	return mappings, nil
}
