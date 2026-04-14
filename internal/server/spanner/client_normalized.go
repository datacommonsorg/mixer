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

// TODO(task): Decouple normalizedClient from spannerDatabaseClient by extracting
// common execution and staleness logic into a shared executor.

// normalizedClient encapsulates the Spanner client for the normalized schema.
type normalizedClient struct {
	sc *spannerDatabaseClient
}

// NewNormalizedClient creates a new normalizedClient.
// It takes a SpannerClient interface and type-asserts it to *spannerDatabaseClient
// to reuse internal helpers like queryStructs. This is a compromise to avoid
// exporting internal implementation details while allowing tests in the golden package
// to construct it.
func NewNormalizedClient(client SpannerClient) (*normalizedClient, error) {
	sc, ok := client.(*spannerDatabaseClient)
	if !ok {
		return nil, fmt.Errorf("NewNormalizedClient: expected *spannerDatabaseClient, got %T", client)
	}
	return &normalizedClient{sc: sc}, nil
}
