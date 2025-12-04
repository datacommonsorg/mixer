// Copyright 2025 Google LLC
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

package maps

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/util"
	"googlemaps.github.io/maps"
)

// MapsClient is a thin facade of googlemaps `maps.Client` for ease of testing.
// If more methods are used, they can be added to the interface as needed.
// See FakeMapsClient for an impl for use in tests.
type MapsClient interface {
	FindPlaceFromText(ctx context.Context, r *maps.FindPlaceFromTextRequest) (maps.FindPlaceFromTextResponse, error)
}

type mapsClient struct {
	client *maps.Client
}

func (c *mapsClient) FindPlaceFromText(ctx context.Context, r *maps.FindPlaceFromTextRequest) (maps.FindPlaceFromTextResponse, error) {
	return c.client.FindPlaceFromText(ctx, r)
}

func NewMapsClient(ctx context.Context, projectID string) (MapsClient, error) {
	apiKey, err := util.ReadLatestSecret(ctx, projectID, util.MapsAPIKeyID)
	if err != nil {
		return nil, err
	}
	client, err := maps.NewClient(maps.WithAPIKey(apiKey))
	if err != nil {
		return nil, err
	}
	return &mapsClient{client}, nil
}
