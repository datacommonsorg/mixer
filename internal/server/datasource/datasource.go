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

package datasource

import (
	"context"

	v2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// DataSourceType represents the type of data source.
type DataSourceType string

const (
	TypeSpanner DataSourceType = "spanner"
	TypeMock    DataSourceType = "mock"
)

// DataSource interface defines the common methods for all data sources.
type DataSource interface {
	Type() DataSourceType
	Node(context.Context, *v2.NodeRequest) (*v2.NodeResponse, error)
}
