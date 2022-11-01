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
	"sync"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"googlemaps.github.io/maps"
)

const (
	maxNumPlacesPerRequest    = 5000
	maxMapsAPICallsInParallel = 20
)

// ResolvePlaceNames implements API for Recon.ResolvePlaceNames.
func ResolvePlaceNames(
	ctx context.Context,
	in *pb.ResolvePlaceNamesRequest,
	store *store.Store,
	mapsClient *maps.Client,
) (*pb.ResolvePlaceNamesResponse, error) {
	if l := len(in.GetPlaces()); l == 0 {
		return nil, fmt.Errorf("empty input")
	} else if l > maxNumPlacesPerRequest {
		return nil, fmt.Errorf(
			"exceeded max number of places per request (%d): %d",
			maxNumPlacesPerRequest, l)
	}

	type placeInfo struct {
		placeName string
		placeType string
	}

	// Load input.
	nameSet := map[string]struct{}{}
	placeInfoSet := map[placeInfo]struct{}{}
	for _, place := range in.GetPlaces() {
		name := place.GetName()
		if name == "" {
			return nil, fmt.Errorf("empty place name")
		}
		nameSet[name] = struct{}{}
		placeInfoSet[placeInfo{name, place.GetType()}] = struct{}{}
	}

	// Get place IDs.
	nameToPlaceIDs, placeIDSet, err := resolvePlaceIDs(ctx, mapsClient, nameSet)
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
	resp := &pb.ResolvePlaceNamesResponse{}
	for info := range placeInfoSet {
		place := &pb.ResolvePlaceNamesResponse_Place{
			Name: info.placeName,
			Type: info.placeType,
		}

		if placeIDs, ok := nameToPlaceIDs.Load(info.placeName); ok {
			allDCIDs := []string{}
			for _, placeID := range placeIDs.([]string) {
				if dcids, ok := placeIDToDCIDs[placeID]; ok {
					allDCIDs = append(allDCIDs, dcids...)
				}
			}
			sort.Strings(allDCIDs)

			if len(allDCIDs) != 0 {
				if info.placeType == "" {
					// No type filtering.
					place.Dcids = allDCIDs
				} else {
					// Type filtering.
					filteredDCIDs := []string{}
					for _, dcid := range allDCIDs {
						typeSet, ok := dcidToTypeSet[dcid]
						if !ok {
							continue
						}
						if _, ok := typeSet[info.placeType]; ok {
							filteredDCIDs = append(filteredDCIDs, dcid)
						}
					}
					place.Dcids = filteredDCIDs
				}
			}
		}

		resp.Places = append(resp.Places, place)
	}

	return resp, nil
}

func resolvePlaceIDs(
	ctx context.Context,
	mapsClient *maps.Client,
	nameSet map[string]struct{},
) (*sync.Map /* name -> place IDs */, *sync.Map /* place ID set */, error) {
	type placeInfo struct {
		name     string
		placeIDs []string
	}
	var wg sync.WaitGroup

	// Send place names to channel.
	nameChan := make(chan string)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(nameChan)
		for name := range nameSet {
			nameChan <- name
		}
	}()

	// Receive and process resolved place IDs.
	nameToPlaceIDs := &sync.Map{}
	placeIDSet := &sync.Map{}
	placeInfoChan := make(chan placeInfo, maxMapsAPICallsInParallel)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for placeInfo := range placeInfoChan {
			nameToPlaceIDs.Store(placeInfo.name, placeInfo.placeIDs)
			for _, placeID := range placeInfo.placeIDs {
				placeIDSet.Store(placeID, struct{}{})
			}
		}
	}()

	// Call Maps API to find place IDs in parallel.
	eg, errCtx := errgroup.WithContext(ctx)
	for i := 0; i < maxMapsAPICallsInParallel; i++ {
		eg.Go(func() error {
			for name := range nameChan {
				placeIDs, err := findPlaceIDsFromName(errCtx, mapsClient, name)
				if err != nil {
					return err
				}
				placeInfoChan <- placeInfo{name: name, placeIDs: placeIDs}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(placeInfoChan)

	wg.Wait()

	return nameToPlaceIDs, placeIDSet, nil
}

func findPlaceIDsFromName(
	ctx context.Context,
	mapsClient *maps.Client,
	name string,
) ([]string, error) {
	resp, err := mapsClient.FindPlaceFromText(ctx, &maps.FindPlaceFromTextRequest{
		Input:     name,
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
	placeIDSet *sync.Map,
	store *store.Store,
) (map[string][]string, map[string]struct{}, error) {
	placeIDs := []string{}
	placeIDSet.Range(func(placeID any, dummy any) bool {
		placeIDs = append(placeIDs, placeID.(string))
		return true
	})

	resolveResp, err := ResolveIds(ctx,
		&pb.ResolveIdsRequest{
			InProp:  "placeId",
			OutProp: "dcid",
			Ids:     placeIDs,
		},
		store)
	if err != nil {
		return nil, nil, err
	}
	placeIDToDCIDs := map[string][]string{}
	dcidSet := map[string]struct{}{}
	for _, entity := range resolveResp.GetEntities() {
		placeIDToDCIDs[entity.GetInId()] = append(placeIDToDCIDs[entity.GetInId()],
			entity.GetOutIds()...)
		for _, dcid := range entity.GetOutIds() {
			dcidSet[dcid] = struct{}{}
		}
	}
	return placeIDToDCIDs, dcidSet, nil
}

func getPlaceTypes(
	ctx context.Context,
	dcidSet map[string]struct{},
	store *store.Store,
) (map[string]map[string]struct{}, error) {
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
