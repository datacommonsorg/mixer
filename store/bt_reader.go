// Copyright 2020 Google LLC
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
	"strings"

	"github.com/datacommonsorg/mixer/util"
)

func btKeyToDcid(key, prefix string) string {
	parts := strings.Split(key, "^")
	return strings.TrimPrefix(parts[0], prefix)
}

// Read Triples from Cloud Bigtable given the node dcid.
func (s *store) ReadTriples(ctx context.Context, dcid string) (
	*TriplesCache, error) {
	key := fmt.Sprintf("%s%s", util.BtTriplesPrefix, dcid)
	row, err := s.btTable.ReadRow(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(row[util.BtFamily]) == 0 {
		return nil, nil
	}
	raw := row[util.BtFamily][0].Value
	jsonRaw, err := util.UnzipAndDecode(string(raw))
	if err != nil {
		return nil, err
	}
	var triples TriplesCache
	err = json.Unmarshal(jsonRaw, &triples)
	if err != nil {
		return nil, err
	}
	return &triples, nil
}
