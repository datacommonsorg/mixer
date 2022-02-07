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

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type prop struct {
	name  string
	isObj bool
}

var obsProps = []prop{
	{"observationAbout", true},
	{"variableMeasured", true},
	{"value", false},
	{"observationDate", false},
	{"observationPeriod", false},
	{"measurementMethod", true},
	{"unit", true},
	{"scalingFactor", false},
	{"samplePopulation", true},
	{"location", true},
}

func getObsTriples(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	obsDcids []string) (map[string][]*model.Triple, error) {
	dcidList := ""
	for _, dcid := range obsDcids {
		dcidList += fmt.Sprintf("\"%s\" ", dcid)
	}
	selectStatment := "SELECT ?o ?provenance "
	tripleStatment := "?o typeOf StatVarObservation . ?o provenance ?provenance . "
	for _, prop := range obsProps {
		selectStatment += fmt.Sprintf("?%s ", prop.name)
		tripleStatment += fmt.Sprintf("?o %s ?%s . ", prop.name, prop.name)
	}
	tripleStatment += fmt.Sprintf("?o dcid (%s)", dcidList)
	sparql := fmt.Sprintf(
		`%s
				WHERE {
					%s
				}
				`, selectStatment, tripleStatment,
	)
	resp, err := translator.Query(ctx, &pb.QueryRequest{Sparql: sparql}, metadata, store)
	if err != nil {
		return nil, err
	}
	result := map[string][]*model.Triple{}
	for _, row := range resp.GetRows() {
		dcid := row.GetCells()[0].Value
		prov := row.GetCells()[1].Value
		objDcids := []string{}
		objTriples := map[string]*model.Triple{}
		for i, prop := range obsProps {
			objCell := row.GetCells()[i+2].Value
			if objCell != "" {
				if prop.isObj {
					// The object is a node; need to fetch the name.
					objDcid := objCell
					objDcids = append(objDcids, objDcid)
					objTriples[objDcid] = &model.Triple{
						SubjectID:    dcid,
						Predicate:    prop.name,
						ObjectID:     objDcid,
						ProvenanceID: prov,
					}
				} else {
					result[dcid] = append(result[dcid], &model.Triple{
						SubjectID:    dcid,
						Predicate:    prop.name,
						ObjectValue:  objCell,
						ProvenanceID: prov,
					})
				}
			}
		}
		nameNodes, err := GetPropertyValuesHelper(ctx, store, objDcids, "name", true)
		if err != nil {
			return nil, err
		}
		for prop, nodes := range nameNodes {
			if len(nodes) > 0 {
				objTriples[prop].ObjectName = nodes[0].Value
			}
		}
		// Sort the triples to get determinisic result.
		keys := make([]string, 0)
		for k := range objTriples {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			result[dcid] = append(result[dcid], objTriples[key])
		}
	}
	return result, nil
}

// GetTriples implements API for Mixer.GetTriples.
func GetTriples(
	ctx context.Context,
	in *pb.GetTriplesRequest,
	store *store.Store,
	metadata *resource.Metadata,
) (
	*pb.GetTriplesResponse, error) {
	dcids := in.GetDcids()
	limit := in.GetLimit()

	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing argument: dcids")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	// Need to fetch additional information for observation node.
	var regDcids, obsDcids []string
	for _, dcid := range dcids {
		if strings.HasPrefix(dcid, "dc/o/") {
			obsDcids = append(obsDcids, dcid)
		} else {
			regDcids = append(regDcids, dcid)
		}
	}

	resultsMap := map[string][]*model.Triple{}

	// Regular DCIDs.
	if len(regDcids) > 0 {
		allTriplesCache, err := ReadTriples(ctx, store.BtGroup, bigtable.BuildTriplesKey(regDcids))
		if err != nil {
			return nil, err
		}
		for dcid := range allTriplesCache {
			resultsMap[dcid] = applyLimit(dcid, allTriplesCache[dcid].Triples, limit)
		}
	}

	// Observation DCIDs.
	if len(obsDcids) > 0 {
		obsResult, err := getObsTriples(ctx, store, metadata, obsDcids)
		if err != nil {
			return nil, err
		}
		for k, v := range obsResult {
			resultsMap[k] = append(resultsMap[k], v...)
		}
	}

	// Format the json response and encode it in base64 as necessary.
	jsonRaw, err := json.Marshal(resultsMap)
	if err != nil {
		return nil, err
	}
	return &pb.GetTriplesResponse{Payload: string(jsonRaw)}, nil
}

func convertTriplesCache(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
	var triples model.TriplesCache
	err := json.Unmarshal(jsonRaw, &triples)
	if err != nil {
		return nil, err
	}
	return &triples, nil
}

func applyLimit(
	dcid string, triples []*model.Triple, limit int32) []*model.Triple {
	if triples == nil {
		return []*model.Triple{}
	}
	if limit == 0 { // Default limit value means no further limit.
		return triples
	}

	// Key is {isOut + predicate + neighborType}.
	existTriple := map[string][]*model.Triple{}
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
				existTriple[key] = []*model.Triple{}
			}
			existTriple[key] = append(existTriple[key], t)
		}
	}

	result := []*model.Triple{}
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

// ReadTriples read triples from base cache for multiple dcids.
func ReadTriples(
	ctx context.Context, btGroup *bigtable.Group, rowList cbt.RowList) (
	map[string]*model.TriplesCache, error) {
	// Only use base cache for triples, as branch cache only consists increment
	// stats. This saves time as the triples list size can get big.
	// Re-evaluate this if branch cache involves other triples.
	btDataList, err := bigtable.Read(
		ctx, btGroup, rowList, convertTriplesCache, nil,
	)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*model.TriplesCache)
	for dcid, data := range btDataList[0] {
		if data == nil {
			result[dcid] = nil
		} else {
			result[dcid] = data.(*model.TriplesCache)
		}
	}
	return result, nil
}
