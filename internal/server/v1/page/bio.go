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

package page

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/biopage"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

// BioPage implements API for Mixer.BioPage.
func BioPage(
	ctx context.Context,
	in *pbv1.BioPageRequest,
	store *store.Store,
) (*pb.GraphNodes, error) {
	node := in.GetNode()
	if err := util.CheckValidDCIDs([]string{node}); err != nil {
		return nil, err
	}
	return biopage.GetBioPageDataHelper(ctx, node, store)
}
