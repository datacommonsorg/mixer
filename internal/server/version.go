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

package server

import (
	"context"
	"os"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// GetVersion implements API for Mixer.GetVersion.
func (s *Server) GetVersion(ctx context.Context,
	in *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {

	return &pb.GetVersionResponse{
		Store:    os.Getenv("STORE_PROJECT"),
		BigQuery: os.Getenv("BIG_QUERY"),
		BigTable: os.Getenv("BIG_TABLE"),
		GitHash:  os.Getenv("MIXER_HASH"),
	}, nil
}
