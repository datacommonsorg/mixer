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

package mcf

import (
	"strings"

	"github.com/datacommonsorg/mixer/internal/translator/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ParseMapping parses schema mapping mcf into a list of Mapping struct.
func ParseMapping(mcf, database string) ([]*types.Mapping, error) {
	lines := strings.Split(mcf, "\n")
	mappings := []*types.Mapping{}
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
			m, err := types.NewMapping(head, sub, body, database)
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, m)
		}
	}
	return mappings, nil
}
