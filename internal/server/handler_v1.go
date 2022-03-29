// Copyright 2022 Google LLC
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

package server

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/variables"
)

// Properties implements API for mixer.Properties.
func (s *Server) Properties(
	ctx context.Context, in *pb.PropertiesRequest,
) (*pb.PropertiesResponse, error) {
	return properties.Properties(ctx, in, s.store)
}

// BulkProperties implements API for mixer.BulkProperties.
func (s *Server) BulkProperties(
	ctx context.Context, in *pb.BulkPropertiesRequest,
) (*pb.BulkPropertiesResponse, error) {
	return properties.BulkProperties(ctx, in, s.store)
}

// Variables implements API for mixer.Variables.
func (s *Server) Variables(
	ctx context.Context, in *pb.VariablesRequest,
) (*pb.VariablesResponse, error) {
	return variables.Variables(ctx, in, s.store)
}

// BulkVariables implements API for mixer.BulkVariables.
func (s *Server) BulkVariables(
	ctx context.Context, in *pb.BulkVariablesRequest,
) (*pb.BulkVariablesResponse, error) {
	return variables.BulkVariables(ctx, in, s.store)
}
