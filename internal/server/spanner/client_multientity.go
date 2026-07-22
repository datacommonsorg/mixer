// Copyright 2026 Google LLC
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

package spanner

import "fmt"

// multiEntityClient delegates calls using the multi-entity schema.
type multiEntityClient struct {
	sc           *spannerDatabaseClient
	queryBuilder *multiEntityQueryBuilder
}

// newMultiEntityClient initializes the client from a base SpannerClient with table and query configuration.
func newMultiEntityClient(client SpannerClient, tableConfig TableConfig, queryConfig MultiEntityQueryConfig) (*multiEntityClient, error) {
	sc, ok := client.(*spannerDatabaseClient)
	if !ok {
		return nil, fmt.Errorf("newMultiEntityClient: expected *spannerDatabaseClient, got %T", client)
	}
	queryBuilder, err := NewMultiEntityQueryBuilder(tableConfig, queryConfig, sc.containedInPlaceQueryConfig)
	if err != nil {
		return nil, fmt.Errorf("newMultiEntityClient: %w", err)
	}
	return &multiEntityClient{
		sc:           sc,
		queryBuilder: queryBuilder,
	}, nil
}
