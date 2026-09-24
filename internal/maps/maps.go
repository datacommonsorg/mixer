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

	places "cloud.google.com/go/maps/places/apiv1"
	"cloud.google.com/go/maps/places/apiv1/placespb"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/api/option"
	"google.golang.org/grpc/metadata"
)

// placeIDFieldMask limits the response to place ids, which is all the resolver
// needs. Text Search bills at the highest tier of any field requested, so
// keeping the mask this narrow keeps the call in the cheapest SKU.
const placeIDFieldMask = "places.id"

// MapsClient is a thin facade over the Places API for ease of testing.
// If more methods are used, they can be added to the interface as needed.
// See FakeMapsClient for an impl for use in tests.
type MapsClient interface {
	FindPlaceIDsFromText(ctx context.Context, query string) ([]string, error)
}

type mapsClient struct {
	client *places.Client
}

func (c *mapsClient) FindPlaceIDsFromText(ctx context.Context, query string) ([]string, error) {
	// Text Search requires the field mask to be set outside the request proto.
	ctx = metadata.AppendToOutgoingContext(ctx, "x-goog-fieldmask", placeIDFieldMask)

	resp, err := c.client.SearchText(ctx, &placespb.SearchTextRequest{TextQuery: query})
	if err != nil {
		return nil, err
	}

	placeIDs := make([]string, 0, len(resp.GetPlaces()))
	for _, place := range resp.GetPlaces() {
		placeIDs = append(placeIDs, place.GetId())
	}

	return placeIDs, nil
}

func NewMapsClient(ctx context.Context, projectID string) (MapsClient, error) {
	apiKey, err := util.ReadLatestSecret(ctx, projectID, util.MapsAPIKeyID)
	if err != nil {
		return nil, err
	}
	client, err := places.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		return nil, err
	}
	return &mapsClient{client}, nil
}
