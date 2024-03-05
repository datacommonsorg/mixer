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

package recon

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	// This is a preferred list.  The props ranked higher are preferred over
	// those ranked lower for resolving.
	//
	// NOTE: Needs to be kept in sync with PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS in
	// https://github.com/datacommonsorg/import repo.
	rankedIDProps = []string{
		"dcid",
		"unDataCode",
		"geoId",
		"isoCode",
		"nutsCode",
		"wikidataId",
		"geoNamesId",
		"istatId",
		"austrianMunicipalityKey",
		"indianCensusAreaCode2011",
		"indianCensusAreaCode2001",
		"lgdCode",
		"udiseCode",
		"fips52AlphaCode",
		"countryAlpha3Code",
		"countryNumericCode",
	}
)

// Get the value of a given property, assuming single value.
func getPropVal(node *pb.McfGraph_PropertyValues, prop string) string {
	values, ok := (node.GetPvs())[prop]
	if !ok {
		return ""
	}
	typedValues := values.GetTypedValues()
	if len(typedValues) == 0 {
		return ""
	}
	return typedValues[0].GetValue()
}

// ResolveEntities implements API for ReconServer.ResolveEntities.
func ResolveEntities(
	ctx context.Context, in *pb.ResolveEntitiesRequest, store *store.Store,
) (
	*pb.ResolveEntitiesResponse, error) {
	idKeyToSourceIDs := map[string][]string{}
	sourceIDs := map[string]struct{}{}
	idKeys := []string{}

	// Collect to-be-resolved IDs to rowList and idKeyToSourceID.
	for _, entity := range in.GetEntities() {
		sourceID := entity.GetSourceId()

		// Try to resolve all the supported IDs
		// For the resolved ones, only rely on the one ranked higher.
		switch t := entity.GraphRepresentation.(type) {
		case *pb.EntitySubGraph_SubGraph:
			node, ok := (entity.GetSubGraph().GetNodes())[sourceID]
			if !ok {
				continue
			}
			for _, idProp := range rankedIDProps {
				idVal := getPropVal(node, idProp)
				if idVal == "" {
					continue
				}
				idKey := fmt.Sprintf("%s^%s", idProp, idVal)
				idKeys = append(idKeys, idKey)
				idKeyToSourceIDs[idKey] = append(idKeyToSourceIDs[idKey], sourceID)
			}
		case *pb.EntitySubGraph_EntityIds:
			idStore := map[string]string{} // Map: ID prop -> ID val.
			for _, id := range entity.GetEntityIds().GetIds() {
				idStore[id.GetProp()] = id.GetVal()
			}
			for _, idProp := range rankedIDProps {
				idVal, ok := idStore[idProp]
				if !ok {
					continue
				}
				idKey := fmt.Sprintf("%s^%s", idProp, idVal)
				idKeys = append(idKeys, idKey)
				idKeyToSourceIDs[idKey] = append(idKeyToSourceIDs[idKey], sourceID)
			}
		default:
			return nil, fmt.Errorf("Entity.GraphRepresentation has unexpected type %T", t)
		}
		sourceIDs[sourceID] = struct{}{}
	}

	// Read ReconIdMap cache.
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtReconIDMapPrefix,
		[][]string{idKeys},
		func(jsonRaw []byte) (interface{}, error) {
			var reconEntities pb.ReconEntities
			if err := proto.Unmarshal(jsonRaw, &reconEntities); err != nil {
				return nil, err
			}
			return &reconEntities, nil
		},
	)
	if err != nil {
		return nil, err
	}

	// Source ID -> ID Prop -> ReconEntities.
	reconEntityStore := map[string]map[string]*pb.ReconEntities{}

	// Group resolving cache result by source ID.
	for _, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		for _, row := range btData {
			idKey := fmt.Sprintf("%s^%s", row.Parts[0], row.Parts[1])
			reconEntities := row.Data
			if reconEntities == nil {
				continue
			}
			sourceIDs, ok := idKeyToSourceIDs[idKey]
			if !ok {
				continue
			}
			parts := strings.Split(idKey, "^")
			if len(parts) != 2 {
				return nil, status.Errorf(codes.Internal, "Invalid id key %s", idKey)
			}
			idProp := parts[0]

			for _, sourceID := range sourceIDs {
				if _, ok := reconEntityStore[sourceID]; !ok {
					reconEntityStore[sourceID] = map[string]*pb.ReconEntities{}
				}
				if re := reconEntities.(*pb.ReconEntities); len(re.GetEntities()) > 0 {
					reconEntityStore[sourceID][idProp] = re
				}
			}
		}
		// Only process data from one preferred import group.
		// TODO: merge entities from differente import groups.
		break
	}

	// Assemble response.
	res := &pb.ResolveEntitiesResponse{}
	for sourceID, idProp2ReconEntities := range reconEntityStore {
		var reconEntities *pb.ReconEntities
		for _, idProp := range rankedIDProps {
			if val, ok := idProp2ReconEntities[idProp]; ok {
				reconEntities = val
				break
			}
		}
		if reconEntities == nil {
			continue
		}

		// If it is resolved to multiple DC entities, each resolved entity has an equal probability.
		probability := float64(1.0 / len(reconEntities.GetEntities()))

		resolvedEntity := &pb.ResolveEntitiesResponse_ResolvedEntity{
			SourceId: sourceID,
		}

		for _, entity := range reconEntities.GetEntities() {
			resolvedID := &pb.ResolveEntitiesResponse_ResolvedId{
				Probability: probability,
			}
			for _, id := range entity.GetIds() {
				resolvedID.Ids = append(resolvedID.Ids,
					&pb.IdWithProperty{
						Prop: id.GetProp(),
						Val:  id.GetVal(),
					})
			}
			resolvedEntity.ResolvedIds = append(resolvedEntity.ResolvedIds, resolvedID)
		}

		res.ResolvedEntities = append(res.ResolvedEntities, resolvedEntity)
	}

	// Add entities that are not resolved as empty result.
	for sourceID := range sourceIDs {
		if _, ok := reconEntityStore[sourceID]; ok { // Resolved.
			continue
		}
		res.ResolvedEntities = append(res.ResolvedEntities,
			&pb.ResolveEntitiesResponse_ResolvedEntity{
				SourceId: sourceID,
			})
	}

	// Sort to make the result deterministic.
	sort.Slice(res.ResolvedEntities, func(i, j int) bool {
		return res.ResolvedEntities[i].GetSourceId() > res.ResolvedEntities[j].GetSourceId()
	})

	return res, nil
}
