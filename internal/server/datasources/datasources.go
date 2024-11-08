// Copyright 2024 Google LLC
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

package datasources

import (
	"context"
	"fmt"

	v3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

// DataSources struct uses underlying data sources to respond to API requests.
type DataSources struct {
	sources []datasource.DataSource
}

func NewDataSources(sources []datasource.DataSource) *DataSources {
	return &DataSources{sources: sources}
}

func (ds *DataSources) Node(ctx context.Context, req *v3.NodeRequest) (*v3.NodeResponse, error) {
	if len(ds.sources) == 0 {
		return nil, fmt.Errorf("unimplemented")
	}

	// Currently this simply returns the node response from the first source.
	// TODO: Call data sources and parallel and return a merged response.
	return ds.sources[0].Node(ctx, req)
}
