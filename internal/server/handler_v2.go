// Copyright 2023 Google LLC
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

	v2pv "github.com/datacommonsorg/mixer/internal/server/v2/propertyvalues"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// PropertyValuesV2 implements API for mixer.PropertyValuesV2.
func (s *Server) PropertyValuesV2(
	ctx context.Context, in *pb.PropertyValuesV2Request,
) (*pb.PropertyValuesV2Response, error) {
	return v2pv.API(ctx, in, s.store)
}
