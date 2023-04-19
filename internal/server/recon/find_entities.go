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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
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
	typeOf      string
}

// FindEntities implements API for Mixer.FindEntities.
func FindEntities(
	ctx context.Context,
	in *pb.FindEntitiesRequest,
	store *store.Store,
	mapsClient *maps.Client,
) (*pb.FindEntitiesResponse, error) {
	bulkResp, err := BulkFindEntities(ctx,
		&pb.BulkFindEntitiesRequest{
			Entities: []*pb.BulkFindEntitiesRequest_Entity{
				{
					Description: in.GetDescription(),
					Type:        in.GetType(),
				},
			},
		},
		store,
		mapsClient)
	if err != nil {
		return nil, err
	}

	return &pb.FindEntitiesResponse{
		Dcids: bulkResp.GetEntities()[0].GetDcids(),
	}, nil
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
			continue
		}
		entityInfoSet[entityInfo{description, entity.GetType()}] = struct{}{}
	}

	// Get DCIDs.
	entityInfoToDCIDs, dcidSet, err := resolveDCIDs(
		ctx, mapsClient, store, entityInfoSet)
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
	for entityInfo, dcids := range entityInfoToDCIDs {
		entity := &pb.BulkFindEntitiesResponse_Entity{
			Description: entityInfo.description,
			Type:        entityInfo.typeOf,
		}

		if len(dcids) != 0 {
			if entityInfo.typeOf == "" {
				// No type filtering.
				entity.Dcids = dcids
			} else {
				// Type filtering.
				filteredDCIDs := []string{}
				for _, dcid := range dcids {
					typeSet, ok := dcidToTypeSet[dcid]
					if !ok {
						continue
					}
					if _, ok := typeSet[entityInfo.typeOf]; ok {
						filteredDCIDs = append(filteredDCIDs, dcid)
					}
				}
				entity.Dcids = filteredDCIDs
			}
		}

		resp.Entities = append(resp.Entities, entity)
	}

	// Sort to make results determistic.
	sort.Slice(resp.Entities, func(i, j int) bool {
		if resp.Entities[i].GetDescription() == resp.Entities[j].GetDescription() {
			return resp.Entities[i].GetType() < resp.Entities[j].GetType()
		}
		return resp.Entities[i].GetDescription() < resp.Entities[j].GetDescription()
	})

	return resp, nil
}

// TODO(ws):
//
// Set some debug info so we can tell how we matched (RecognizePlaces vs Maps API).
//
// Consider calling both, if both have results, prefer RecognizePlaces,
// but use Maps API as signal to reorder the results.
func resolveDCIDs(
	ctx context.Context,
	mapsClient *maps.Client,
	store *store.Store,
	entityInfoSet map[entityInfo]struct{},
) (
	map[entityInfo][]string, /* entityInfo -> [DCID] */
	map[string]struct{}, /* [DCID] for all entities */
	error,
) {
	// First try to resolve DCIDs by RecognizePlaces.
	entityInfoToDCIDSet, dcidSet, err := resolveWithRecognizePlaces(
		ctx, store, entityInfoSet)
	if err != nil {
		return nil, nil, err
	}

	// See if there are any entities that cannot be resolved by RecognizePlaces.
	missingEntityInfoSet := map[entityInfo]struct{}{}
	for entityInfo := range entityInfoSet {
		if dcidSet, ok := entityInfoToDCIDSet[entityInfo]; !ok || len(dcidSet) == 0 {
			missingEntityInfoSet[entityInfo] = struct{}{}
		}
	}
	if len(missingEntityInfoSet) > 0 {
		// For entities that cannot be resolved by RecognizePlaces, try Maps API.
		missingEntityInfoToDCIDSet, missingDcidSet, err := resolveWithMapsAPI(
			ctx, mapsClient, store, entityInfoSet)
		if err != nil {
			return nil, nil, err
		}

		// Add the newly resolved entities.
		for e, dSet := range missingEntityInfoToDCIDSet {
			entityInfoToDCIDSet[e] = dSet
		}
		for dcid := range missingDcidSet {
			dcidSet[dcid] = struct{}{}
		}
	}

	// Format the result, transform DCID set to DCID list.
	res := map[entityInfo][]string{}
	for e, dSet := range entityInfoToDCIDSet {
		res[e] = []string{}
		for dcid := range dSet {
			res[e] = append(res[e], dcid)
		}
	}
	return res, dcidSet, nil
}

func resolveWithRecognizePlaces(
	ctx context.Context,
	store *store.Store,
	entityInfoSet map[entityInfo]struct{},
) (
	map[entityInfo]map[string]struct{}, /* entityInfo -> DCID set */
	map[string]struct{}, /* [DCID] for all entities */
	error,
) {
	// Check if the query fully matches any place names.
	// NOTE: names also include "selfName containingPlaceName", e.g. "Brussels Belgium".
	hasQueryNameMatch := func(dcid, query string) bool {
		format := func(n string) string {
			s := strings.ReplaceAll(strings.ToLower(n), " ", "")
			return strings.ReplaceAll(s, ",", "")
		}
		names, ok := store.RecogPlaceStore.DcidToNames[dcid]
		if !ok {
			return false
		}
		for _, name := range names {
			if format(query) == format(name) {
				return true
			}
		}
		return false
	}

	req := &pb.RecognizePlacesRequest{Queries: []string{}}
	descriptionToType := map[string]string{}
	for e := range entityInfoSet {
		req.Queries = append(req.Queries, e.description)
		descriptionToType[e.description] = e.typeOf
	}

	resp, err := RecognizePlaces(ctx, req, store)
	if err != nil {
		return nil, nil, err
	}

	entityInfoToDCIDSet := map[entityInfo]map[string]struct{}{}
	dcidSet := map[string]struct{}{}

	for query, items := range resp.GetQueryItems() {
		e := entityInfo{description: query, typeOf: descriptionToType[query]}
		entityInfoToDCIDSet[e] = map[string]struct{}{}
		for _, item := range items.GetItems() {
			for _, place := range item.GetPlaces() {
				dcid := place.GetDcid()
				if hasQueryNameMatch(dcid, query) {
					entityInfoToDCIDSet[e][dcid] = struct{}{}
					dcidSet[dcid] = struct{}{}
				}
			}
		}
	}

	return entityInfoToDCIDSet, dcidSet, nil
}

func resolveWithMapsAPI(
	ctx context.Context,
	mapsClient *maps.Client,
	store *store.Store,
	entityInfoSet map[entityInfo]struct{},
) (
	map[entityInfo]map[string]struct{}, /* entityInfo -> DCID set */
	map[string]struct{}, /* [DCID] for all entities */
	error,
) {
	// Get place IDs.
	entityInfoToPlaceIDs, placeIDSet, err := resolvePlaceIDsFromDescriptions(
		ctx, mapsClient, entityInfoSet)
	if err != nil {
		return nil, nil, err
	}

	// Resolve place IDs to get DCIDs.
	placeIDToDCIDs, dcidSet, err := resolveDCIDsFromPlaceIDs(ctx, placeIDSet, store)
	if err != nil {
		return nil, nil, err
	}

	res := map[entityInfo]map[string]struct{}{}
	for entityInfo, placeIDs := range entityInfoToPlaceIDs {
		if _, ok := res[entityInfo]; !ok {
			res[entityInfo] = map[string]struct{}{}
		}
		for _, placeID := range placeIDs {
			if dcids, ok := placeIDToDCIDs[placeID]; ok {
				for _, dcid := range dcids {
					res[entityInfo][dcid] = struct{}{}
				}
			}
		}
	}

	return res, dcidSet, nil
}

func resolvePlaceIDsFromDescriptions(
	ctx context.Context,
	mapsClient *maps.Client,
	entityInfoSet map[entityInfo]struct{},
) (
	map[entityInfo][]string, /* entityInfo -> [place ID] */
	map[string]struct{}, /* [place ID] for all entities */
	error,
) {
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
				usedPlaceIds := []string{}
				if len(placeIDs) > 0 {
					// Only keep the first place ID, as the rest ones are usually much less accurate.
					usedPlaceIds = []string{placeIDs[0]}
				}
				resolveResultChan <- resolveResult{
					entityInfo: &entityInfo,
					placeIDs:   usedPlaceIds}
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
	if t := entityInfo.typeOf; t != "" {
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

func resolveDCIDsFromPlaceIDs(
	ctx context.Context,
	placeIDSet map[string]struct{},
	store *store.Store,
) (
	map[string][]string, /* Place ID -> [DCID] */
	map[string]struct{}, /* [DCID] for all place IDs */
	error,
) {
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
	error,
) {
	resp, err := propertyvalues.BulkPropertyValues(ctx,
		&pbv1.BulkPropertyValuesRequest{
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
