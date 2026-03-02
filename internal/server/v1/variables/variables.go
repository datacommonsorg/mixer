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

package variables

import (
	"context"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/statvar/fetcher"
	"github.com/datacommonsorg/mixer/internal/util"

	"github.com/datacommonsorg/mixer/internal/store"
)

// Variables implements API for Mixer.Variables.
func Variables(
	ctx context.Context,
	in *pbv1.VariablesRequest,
	store *store.Store,
) (*pbv1.VariablesResponse, error) {
	entity := in.GetEntity()
	if err := util.CheckValidDCIDs([]string{entity}); err != nil {
		return nil, err
	}
	entityToStatVars, err := fetcher.FetchEntityVariables(ctx, store, []string{entity})
	if err != nil {
		return nil, err
	}

	resp := &pbv1.VariablesResponse{Entity: entity}
	statVars, ok := entityToStatVars[entity]
	if !ok {
		return resp, nil
	}
	resp.Variables = statVars.StatVars

	return resp, nil
}
