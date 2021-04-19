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
	"fmt"
	"os"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// GetVersion implements API for Mixer.GetVersion.
func (s *Server) GetVersion(ctx context.Context,
	in *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {

	store := os.Getenv("STORE_PROJECT")
	bigQuery := os.Getenv("BIG_QUERY")
	bigTable := os.Getenv("BIG_TABLE")
	mixer := os.Getenv("MIXER_HASH")

	text := fmt.Sprintf(
		"store:%s\nmixer:%s\nbigquery:%s\nbigtable:%s",
		store, mixer, bigQuery, bigTable)
	out := pb.GetVersionResponse{Info: text}
	return &out, nil
}
