// Copyright 2021 Google LLC
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

package parser

import (
	"strings"

	"github.com/datacommonsorg/mixer/internal/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Column represents a csv column' node and property.
type Column struct {
	Node     string
	Property string
}

// TableSchema Represents the schema of one table
type TableSchema struct {
	// Keyed by column name.
	ColumnInfo map[string]*Column
	// Keyed by node name and property.
	NodeSchema map[string]map[string]string
}

// ParseTmcf parses TMCF into a map with key of the table name, and value being the
// TableSchema struct.
func ParseTmcf(tmcf string) (map[string]*TableSchema, error) {
	result := map[string]*TableSchema{}
	lines := strings.Split(tmcf, "\n")
	var table string
	var node string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Comment starts with "#"
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) < 2 {
			return nil, status.Errorf(
				codes.Internal, "invalid tmcf:\n%s", tmcf)
		}
		head := strings.TrimSpace(parts[0])
		body := strings.TrimSpace(parts[1])

		// Node entity mapping
		if strings.HasPrefix(body, base.PreE) {
			if head != "Node" {
				return nil, status.Errorf(codes.Internal, "Only supports fully resolved TMCF import")
			}
			parts := strings.SplitN(strings.TrimPrefix(body, base.PreE), base.Arrow, 2)
			if len(parts) != 2 {
				return nil, status.Errorf(codes.Internal, "Invalid input for Entity:\n%s", line)
			}
			table = parts[0]
			node = parts[1]
			result[table] = &TableSchema{
				ColumnInfo: map[string]*Column{},
				NodeSchema: map[string]map[string]string{},
			}
		} else if strings.HasPrefix(body, base.PreC) {
			// Column mapping
			parts := strings.SplitN(strings.TrimPrefix(body, base.PreC), base.Arrow, 2)
			if len(parts) != 2 {
				return nil, status.Errorf(codes.Internal, "Invalid input for Column:\n%s", line)
			}
			if table == "" || node == "" || table != parts[0] {
				return nil, status.Errorf(codes.Internal, "Invalid input for Column:\n%s", line)
			}
			result[table].ColumnInfo[parts[1]] = &Column{Node: node, Property: head}
		} else if strings.HasPrefix(body, base.DCS) {
			// Schema mapping
			schema := strings.TrimPrefix(body, base.DCS)
			if _, ok := result[table].NodeSchema[parts[1]]; !ok {
				result[table].NodeSchema[node] = map[string]string{}
			}
			result[table].NodeSchema[node][head] = schema
		}
	}
	return result, nil
}
