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

// Package recon contains code for recon.
package recon

import (
	"context"
	"fmt"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"googlemaps.github.io/maps"
)

const (
	maxNumEntitiesPerRequest  = 5000
	maxMapsAPICallsInParallel = 25
)

type entityInfo struct {
	description string
	entityType  string
}

// BulkFindEntities implements API for Mixer.BulkFindEntities.
func BulkFindEntities(
	ctx context.Context,
	in *pb.BulkFindEntitiesRequest,
	store *store.Store,
	mapsClient *maps.Client,
) (*pb.BulkFindEntitiesResponse, error) {
	if l := len(in.GetEntities()); l == 0 {
		return nil, fmt.Errorf("empty input")
	} else if l > maxNumEntitiesPerRequest {
		return nil, fmt.Errorf(
			"exceeded max number of entities per request (%d): %d",
			maxNumEntitiesPerRequest, l)
	}

	// Load input.
	entityInfoSet := map[entityInfo]struct{}{}
	for _, entity := range in.GetEntities() {
		description := entity.GetDescription()
		if description == "" {
			return nil, fmt.Errorf("empty entity description")
		}
		entityInfoSet[entityInfo{description, entity.GetType()}] = struct{}{}
	}

	// Get place IDs.
	entityInfoToPlaceIDs, placeIDSet, err := resolvePlaceIDs(ctx, mapsClient, entityInfoSet)
	if err != nil {
		return nil, err
	}

	// Resolve place IDs to get DCIDs.
	placeIDToDCIDs, dcidSet, err := resolveDCIDs(ctx, placeIDSet, store)
	if err != nil {
		return nil, err
	}

	// Get types of the DCIDs.
	dcidToTypeSet, err := getPlaceTypes(ctx, dcidSet, store)
	if err != nil {
		return nil, err
	}

	// Assemble results.
	resp := &pb.BulkFindEntitiesResponse{}
	for entityInfo, placeIDs := range entityInfoToPlaceIDs {
		entity := &pb.BulkFindEntitiesResponse_Entity{
			Description: entityInfo.description,
			Type:        entityInfo.entityType,
		}

		allDCIDs := []string{}
		for _, placeID := range placeIDs {
			if dcids, ok := placeIDToDCIDs[placeID]; ok {
				allDCIDs = append(allDCIDs, dcids...)
			}
		}

		if len(allDCIDs) != 0 {
			if entityInfo.entityType == "" {
				// No type filtering.
				entity.Dcids = allDCIDs
			} else {
				// Type filtering.
				filteredDCIDs := []string{}
				for _, dcid := range allDCIDs {
					typeSet, ok := dcidToTypeSet[dcid]
					if !ok {
						continue
					}
					if _, ok := typeSet[entityInfo.entityType]; ok {
						filteredDCIDs = append(filteredDCIDs, dcid)
					}
				}
				entity.Dcids = filteredDCIDs
			}
		}

		resp.Entities = append(resp.Entities, entity)
	}

	return resp, nil
}

func resolvePlaceIDs(
	ctx context.Context,
	mapsClient *maps.Client,
	entityInfoSet map[entityInfo]struct{},
) (
	map[entityInfo][]string, /* entityInfo -> [place ID] */
	map[string]struct{}, /* [place ID] for all entities */
	error) {
	type resolveResult struct {
		entityInfo *entityInfo
		placeIDs   []string
	}

	// Distribute entityInfoSet to maxMapsAPICallsInParallel shards for parallel processing.
	entityInfoListShards := make([][]entityInfo, maxMapsAPICallsInParallel)
	idx := 0
	for entityInfo := range entityInfoSet {
		entityInfoListShards[idx] = append(entityInfoListShards[idx], entityInfo)
		idx++
		if idx >= maxMapsAPICallsInParallel {
			idx = 0
		}
	}

	// The channel to receive results from parallel workers.
	resolveResultChan := make(chan resolveResult, len(entityInfoSet))

	// Worker function.
	mapsAPICallWorkerFunc := func(ctx context.Context, i int) func() error {
		return func() error {
			for _, entityInfo := range entityInfoListShards[i] {
				placeIDs, err := findPlaceIDsForEntity(ctx, mapsClient, &entityInfo)
				if err != nil {
					return err
				}
				resolveResultChan <- resolveResult{entityInfo: &entityInfo, placeIDs: placeIDs}
			}
			return nil
		}
	}

	// Call Maps API to find place IDs in parallel.
	// The errors in the Goroutines need to be captured, so we use errgroup.
	eg, errCtx := errgroup.WithContext(ctx)
	for i := 0; i < maxMapsAPICallsInParallel; i++ {
		eg.Go(mapsAPICallWorkerFunc(errCtx, i))
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(resolveResultChan)

	// Read out the results sent by workers.
	entityInfoToPlaceIDs := map[entityInfo][]string{}
	placeIDSet := map[string]struct{}{}
	for res := range resolveResultChan {
		entityInfoToPlaceIDs[*res.entityInfo] = res.placeIDs
		for _, placeID := range res.placeIDs {
			placeIDSet[placeID] = struct{}{}
		}
	}

	return entityInfoToPlaceIDs, placeIDSet, nil
}

func findPlaceIDsForEntity(
	ctx context.Context,
	mapsClient *maps.Client,
	entityInfo *entityInfo,
) ([]string, error) {
	// When type is supplied, we append it to the description to increase the accuracy.
	input := entityInfo.description
	if t := entityInfo.entityType; t != "" {
		input += (" " + t)
	}

	resp, err := mapsClient.FindPlaceFromText(ctx, &maps.FindPlaceFromTextRequest{
		Input:     input,
		InputType: maps.FindPlaceFromTextInputTypeTextQuery,
		Fields:    []maps.PlaceSearchFieldMask{maps.PlaceSearchFieldMaskPlaceID},
	})
	if err != nil {
		return nil, err
	}

	placeIDs := []string{}
	for _, candidate := range resp.Candidates {
		placeIDs = append(placeIDs, candidate.PlaceID)
	}

	return placeIDs, nil
}

func resolveDCIDs(
	ctx context.Context,
	placeIDSet map[string]struct{},
	store *store.Store,
) (
	map[string][]string, /* Place ID -> [DCID] */
	map[string]struct{}, /* [DCID] for all place IDs */
	error) {
	resolveResp, err := ResolveIds(ctx,
		&pb.ResolveIdsRequest{
			InProp:  "placeId",
			OutProp: "dcid",
			Ids:     util.StringSetToSlice(placeIDSet),
		},
		store)
	if err != nil {
		return nil, nil, err
	}
	placeIDToDCIDs := map[string][]string{}
	dcidSet := map[string]struct{}{}
	for _, entity := range resolveResp.GetEntities() {
		dcids := entity.GetOutIds()

		// Sort to make the result deterministic.
		sort.Strings(dcids)

		placeIDToDCIDs[entity.GetInId()] = append(placeIDToDCIDs[entity.GetInId()],
			dcids...)
		for _, dcid := range dcids {
			dcidSet[dcid] = struct{}{}
		}
	}
	return placeIDToDCIDs, dcidSet, nil
}

func getPlaceTypes(
	ctx context.Context,
	dcidSet map[string]struct{},
	store *store.Store,
) (
	map[string]map[string]struct{}, /* DCID -> {type} */
	error) {
	resp, err := propertyvalues.BulkPropertyValues(ctx,
		&pb.BulkPropertyValuesRequest{
			Property:  "typeOf",
			Nodes:     util.StringSetToSlice(dcidSet),
			Direction: util.DirectionOut,
		},
		store)
	if err != nil {
		return nil, err
	}
	dcidToTypeSet := map[string]map[string]struct{}{}
	for _, nodeInfo := range resp.GetData() {
		dcidToTypeSet[nodeInfo.GetNode()] = map[string]struct{}{}
		for _, entityInfo := range nodeInfo.GetValues() {
			dcidToTypeSet[nodeInfo.GetNode()][entityInfo.GetDcid()] = struct{}{}
		}
	}
	return dcidToTypeSet, nil
}
