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

package data

import (
	"context"
	"net/http"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/sqlite/writer"
)

// Import implements API for Mixer.Import.
func Import(
	ctx context.Context,
	in *pb.ImportRequest,
	metadata *resource.Metadata,
	httpClient *http.Client,
) (*pb.ImportResponse, error) {
	if err := writer.Write(in.GetInputDir(),
		in.GetOutputDir(),
		metadata,
		httpClient); err != nil {
		return nil, err
	}
	return &pb.ImportResponse{Success: true}, nil
}
