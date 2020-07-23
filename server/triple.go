// Copyright 2020 Google LLC
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
)

const (
	obsAncestorTypeObservedNode = "0"
	obsAncestorTypeComparedNode = "1"
)

// GetTriplesPost implements API for Mixer.GetTriplesPost.
func (s *Server) GetTriplesPost(ctx context.Context,
	in *pb.GetTriplesRequest) (*pb.GetTriplesResponse, error) {
	return s.GetTriples(ctx, in)
}

// GetTriples implements API for Mixer.GetTriples.
func (s *Server) GetTriples(ctx context.Context, in *pb.GetTriplesRequest) (
	*pb.GetTriplesResponse, error) {
	dcids := in.GetDcids()
	limit := in.GetLimit()

	if len(dcids) == 0 {
		return nil, fmt.Errorf("must provide DCIDs")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	// Need to fetch addtional information for observation and population node.
	var regDcids, obsDcids, popDcids []string
	for _, dcid := range dcids {
		if strings.HasPrefix(dcid, "dc/o/") {
			obsDcids = append(obsDcids, dcid)
		} else {
			regDcids = append(regDcids, dcid)
			if strings.HasPrefix(dcid, "dc/p/") {
				popDcids = append(popDcids, dcid)
			}
		}
	}

	resultsMap := map[string][]*Triple{}

	// Regular DCIDs.
	if len(regDcids) > 0 {
		allTriplesCache, err := readTriples(ctx, s.btTable, buildTriplesKey(regDcids))
		if err != nil {
			return nil, err
		}
		for dcid := range allTriplesCache {
			resultsMap[dcid] = applyLimit(dcid, allTriplesCache[dcid].Triples, limit)
		}
	}
	// Regular triples cache from memcache for population data
	if len(popDcids) > 0 {
		dataMap := s.memcache.ReadParallel(
			buildTriplesKey(popDcids), convertTriplesCache)
		for dcid, data := range dataMap {
			resultsMap[dcid] = data.(*TriplesCache).Triples
		}
	}

	// Observation DCIDs.
	if len(obsDcids) > 0 {
		for _, param := range []struct {
			predKey, pred string
		}{
			{obsAncestorTypeObservedNode, "observedNode"},
			{obsAncestorTypeComparedNode, "comparedNode"},
		} {
			rowList := buildObservedNodeKey(obsDcids, param.predKey)
			dataMap, err := bigTableReadRowsParallel(
				ctx, s.btTable, rowList,
				func(dcid string, raw []byte) (interface{}, error) {
					return string(raw), nil
				}, TokenTypeDcid)
			if err != nil {
				return nil, err
			}
			// If using branch cache, then check the branch cache as well.
			var branchDataMap map[string]interface{}
			if in.GetOption().GetCacheChoice() != pb.Option_BASE_CACHE_ONLY {
				branchDataMap = s.memcache.ReadParallel(rowList,
					func(dcid string, raw []byte) (interface{}, error) {
						return string(raw), nil
					})
			}
			// Map from observation dcid to observedNode dcid.
			observedNodeMap := map[string]string{}
			for dcid, data := range dataMap {
				observedNodeMap[dcid] = data.(string)
			}
			for dcid, data := range branchDataMap {
				observedNodeMap[dcid] = data.(string)
			}
			// Get the observedNode names.
			observedNodes := []string{}
			for _, dcid := range observedNodeMap {
				observedNodes = append(observedNodes, dcid)
			}
			nameRowList := buildPropertyValuesKey(observedNodes, "name", true)
			nameNodes, err := readPropertyValues(ctx, s.btTable, nameRowList)
			if err != nil {
				return nil, err
			}
			for dcid, observedNode := range observedNodeMap {
				if _, exist := resultsMap[dcid]; !exist {
					resultsMap[dcid] = []*Triple{}
				}
				name := observedNode
				if len(nameNodes[observedNode]) > 0 {
					name = nameNodes[observedNode][0].Value
				}
				resultsMap[dcid] = append(resultsMap[dcid], &Triple{
					SubjectID:  dcid,
					Predicate:  param.pred,
					ObjectID:   observedNode,
					ObjectName: name,
				})
			}
		}
	}

	// Add PVs for population nodes.
	if len(popDcids) > 0 {
		rowList := buildPopPVKey(popDcids)
		dataMap, err := bigTableReadRowsParallel(
			ctx, s.btTable, rowList, convertPopTriples, TokenTypeDcid)
		if err != nil {
			return nil, err
		}
		for dcid, data := range dataMap {
			resultsMap[dcid] = append(resultsMap[dcid], data.([]*Triple)...)
		}
		// No data found in base cache, look in branch cache
		if len(dataMap) == 0 {
			branchDataMap := s.memcache.ReadParallel(rowList, convertPopTriples)
			for dcid, data := range branchDataMap {
				resultsMap[dcid] = append(resultsMap[dcid], data.([]*Triple)...)
			}
		}
	}

	// Format the json response and encode it in base64 as necessary.
	jsonRaw, err := json.Marshal(resultsMap)
	if err != nil {
		return nil, err
	}
	return &pb.GetTriplesResponse{Payload: string(jsonRaw)}, nil
}

func convertTriplesCache(dcid string, jsonRaw []byte) (interface{}, error) {
	var triples TriplesCache
	err := json.Unmarshal(jsonRaw, &triples)
	if err != nil {
		return nil, err
	}
	return &triples, nil
}

func convertPopTriples(dcid string, jsonRaw []byte) (interface{}, error) {
	jsonVal := string(jsonRaw)
	parts := strings.Split(jsonVal, "^")
	if len(parts) == 0 || len(parts)%2 != 0 {
		return nil, fmt.Errorf("wrong number of PVs: %v", jsonVal)
	}
	triples := []*Triple{}
	triples = append(triples, &Triple{
		SubjectID:   dcid,
		Predicate:   "numConstraints",
		ObjectValue: strconv.Itoa(len(parts) / 2),
	})
	for i := 0; i < len(parts); i = i + 2 {
		triples = append(triples, &Triple{
			SubjectID: dcid,
			Predicate: parts[i],
			ObjectID:  parts[i+1],
		})
	}
	return triples, nil
}

func applyLimit(
	dcid string, triples []*Triple, limit int32) []*Triple {
	if triples == nil {
		return []*Triple{}
	}
	if limit == 0 { // Default limit value means no further limit.
		return triples
	}

	// Key is {isOut + predicate + neighborType}.
	existTriple := map[string][]*Triple{}
	for _, t := range triples {
		isOut := "0"
		neighborTypes := t.SubjectTypes
		if t.SubjectID == dcid {
			isOut = "1"
			neighborTypes = t.ObjectTypes
		}

		for _, nt := range neighborTypes {
			key := isOut + t.Predicate + nt
			if _, ok := existTriple[key]; !ok {
				existTriple[key] = []*Triple{}
			}
			existTriple[key] = append(existTriple[key], t)
		}
	}

	result := []*Triple{}
	for _, triples := range existTriple {
		var count int32
		for _, t := range triples {
			result = append(result, t)
			count++
			if count == limit {
				break
			}
		}
	}
	return result
}

// ReadTriples read triples from Cloud Bigtable for multiple dcids.
func readTriples(
	ctx context.Context, btTable *bigtable.Table, rowList bigtable.RowList) (
	map[string]*TriplesCache, error) {
	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertTriplesCache, TokenTypeDcid)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*TriplesCache)
	for dcid, data := range dataMap {
		result[dcid] = data.(*TriplesCache)
	}
	return result, nil
}
