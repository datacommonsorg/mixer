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
	"fmt"
	"sort"
	"strings"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/server/convert"
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
	obsDcids []string,
) (map[string][]*pb.Triple, error) {
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
	resp, err := translator.Query(
		ctx, &pb.QueryRequest{Sparql: sparql}, metadata, store)
	if err != nil {
		return nil, err
	}
	result := map[string][]*pb.Triple{}
	for _, row := range resp.GetRows() {
		dcid := row.GetCells()[0].Value
		prov := row.GetCells()[1].Value
		objDcids := []string{}
		objTriples := map[string]*pb.Triple{}
		for i, prop := range obsProps {
			objCell := row.GetCells()[i+2].Value
			if objCell != "" {
				if prop.isObj {
					// The object is a node; need to fetch the name.
					objDcid := objCell
					objDcids = append(objDcids, objDcid)
					objTriples[objDcid] = &pb.Triple{
						SubjectId:    dcid,
						Predicate:    prop.name,
						ObjectId:     objDcid,
						ProvenanceId: prov,
					}
				} else {
					result[dcid] = append(result[dcid], &pb.Triple{
						SubjectId:    dcid,
						Predicate:    prop.name,
						ObjectValue:  objCell,
						ProvenanceId: prov,
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
) (*pb.GetTriplesResponse, error) {
	dcids := in.GetDcids()
	limit := int(in.GetLimit())

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

	result := &pb.GetTriplesResponse{Triples: make(map[string]*pb.Triples)}
	var err error
	// Regular DCIDs.
	if len(regDcids) > 0 {
		result, err = ReadTriples(ctx, store.BtGroup, bigtable.BuildTriplesKey(regDcids))
		if err != nil {
			return nil, err
		}
		for dcid, triples := range result.Triples {
			applyLimit(dcid, triples, limit)
		}
	}
	// Observation DCIDs.
	if len(obsDcids) > 0 {
		obsResult, err := getObsTriples(ctx, store, metadata, obsDcids)
		if err != nil {
			return nil, err
		}
		for k, v := range obsResult {
			if result.Triples[k] == nil {
				result.Triples[k] = &pb.Triples{}
			}
			result.Triples[k].Triples = append(result.Triples[k].Triples, v...)
		}
	}
	return result, nil
}

// ReadTriples read triples from base cache for multiple dcids.
func ReadTriples(ctx context.Context, btGroup *bigtable.Group, rowList cbt.RowList) (
	*pb.GetTriplesResponse, error) {
	baseDataList, _, err := bigtable.Read(
		ctx, btGroup, rowList, convert.ToTriples, nil, true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := &pb.GetTriplesResponse{Triples: make(map[string]*pb.Triples)}
	// dcid -> predicate -> id/value
	visited := map[string]map[string]map[string]struct{}{}
	for _, baseData := range baseDataList {
		for dcid, data := range baseData {
			triples, ok := data.(*pb.Triples)
			if !ok {
				return nil, status.Error(codes.Internal, "Error reading triples cache")
			}
			if triples.Triples != nil {
				// Non import group case.
				result.Triples[dcid] = triples
			} else {
				if _, ok := result.Triples[dcid]; !ok {
					result.Triples[dcid] = &pb.Triples{
						InNodes:  map[string]*pb.EntityInfoCollection{},
						OutNodes: map[string]*pb.EntityInfoCollection{},
					}
				}
				if _, ok := visited[dcid]; !ok {
					visited[dcid] = map[string]map[string]struct{}{}
				}
				// This is import group case, since there are multiple cache data.
				for pred, entities := range triples.OutNodes {
					// For out nodes, only add data to result if it does not exist.
					if _, ok := result.Triples[dcid].OutNodes[pred]; !ok {
						result.Triples[dcid].OutNodes[pred] = entities
					}
				}
				for pred, c := range triples.InNodes {
					if _, ok := visited[dcid][pred]; !ok {
						visited[dcid][pred] = map[string]struct{}{}
					}
					// For in nodes, merge entities from different tables.
					if _, ok := result.Triples[dcid].InNodes[pred]; !ok {
						result.Triples[dcid].InNodes[pred] = c
						for _, e := range c.Entities {
							visited[dcid][pred][e.Dcid] = struct{}{}
						}
					} else {
						for _, e := range c.Entities {
							// Check if a duplicate node has been added to the result.
							// Duplication is based on either the DCID or the value.
							if _, ok := visited[dcid][pred][e.Dcid]; !ok {
								result.Triples[dcid].InNodes[pred].Entities = append(
									result.Triples[dcid].InNodes[pred].Entities,
									e,
								)
								visited[dcid][pred][e.Dcid] = struct{}{}
							}
						}
					}
				}
			}
		}
	}
	return result, nil
}

// Filter triples in place.
func applyLimit(dcid string, t *pb.Triples, limit int) {
	if t == nil {
		return
	}
	// Default limit value means no further limit.
	if limit == 0 {
		return
	}
	if t.Triples != nil {
		// This is the old triples cache.
		// Key is {isOut + predicate + neighborType}.
		existTriple := map[string][]*pb.Triple{}
		for _, t := range t.Triples {
			isOut := "0"
			neighborTypes := t.SubjectTypes
			if t.SubjectId == dcid {
				isOut = "1"
				neighborTypes = t.ObjectTypes
			}

			for _, nt := range neighborTypes {
				key := isOut + t.Predicate + nt
				if _, ok := existTriple[key]; !ok {
					existTriple[key] = []*pb.Triple{}
				}
				existTriple[key] = append(existTriple[key], t)
			}
		}

		filtered := []*pb.Triple{}
		for _, triples := range existTriple {
			var count int
			for _, t := range triples {
				filtered = append(filtered, t)
				count++
				if count == limit {
					break
				}
			}
		}
		t.Triples = filtered
	} else {
		// This is the import group mdoe.
		//
		// Apply the filtering
		for _, target := range []map[string]*pb.EntityInfoCollection{
			t.OutNodes,
			t.InNodes,
		} {
			for _, c := range target {
				if len(c.Entities) < limit {
					continue
				}
				tmp := map[string][]*pb.EntityInfo{}
				for _, e := range c.Entities {
					nt := e.Types[0]
					if len(tmp[nt]) < limit {
						tmp[nt] = append(tmp[nt], e)
					}
				}
				c.Entities = []*pb.EntityInfo{}
				for _, entities := range tmp {
					c.Entities = append(c.Entities, entities...)
				}
			}
		}
	}
}
