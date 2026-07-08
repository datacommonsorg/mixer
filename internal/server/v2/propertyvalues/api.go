// Copyright 2023 Google LLC
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

// Package propertyvalues is for V2 property values API.
package propertyvalues

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	v1pv "github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2p "github.com/datacommonsorg/mixer/internal/server/v2/properties"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
)

const (
	CHAIN = "+"
)

// PropertyValues is the V2 property values API implementation entry point.
func PropertyValues(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	nodes []string,
	properties []string,
	direction string,
	limit int,
	reqToken string,
	arc *v2.Arc,
) (*pbv2.NodeResponse, error) {
	obsNodes := []string{}
	regularNodes := []string{}
	for _, n := range nodes {
		if strings.HasPrefix(n, "dc/o") {
			obsNodes = append(obsNodes, n)
		} else {
			regularNodes = append(regularNodes, n)
		}
	}

	res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}

	if len(obsNodes) > 0 {
		propertySet := map[string]struct{}{}
		for _, p := range properties {
			propertySet[p] = struct{}{}
		}
		tripleResp, err := node.GetObsTriples(ctx, store, metadata, obsNodes)
		if err != nil {
			return nil, err
		}
		for dcid, tripleList := range tripleResp {
			res.Data[dcid] = &pbv2.LinkedGraph{Arcs: map[string]*pbv2.Nodes{}}
			for _, t := range tripleList {
				// NOTE: len(properties) == 0 means this is for triples, so no filtering here.
				if _, ok := propertySet[t.Predicate]; ok || len(properties) == 0 {
					res.Data[dcid].Arcs[t.Predicate] = &pbv2.Nodes{
						Nodes: []*pb.EntityInfo{
							{
								Name:         t.ObjectName,
								Value:        t.ObjectValue,
								Types:        t.ObjectTypes,
								Dcid:         t.ObjectId,
								ProvenanceId: t.ProvenanceId,
							},
						},
					}
				}
			}
		}
	}

	if len(regularNodes) > 0 {
		if len(properties) == 0 {
			// For triples, get all properties of regular nodes.
			propRes, err := v2p.API(ctx, store, regularNodes, direction)
			if err != nil {
				return nil, err
			}
			for _, data := range propRes.GetData() {
				properties = util.MergeDedupe(properties, data.GetProperties())
			}
		}

		data, pi, err := v1pv.Fetch(
			ctx,
			store,
			regularNodes,
			properties,
			limit,
			reqToken,
			direction,
		)
		if err != nil {
			return nil, err
		}
		for _, n := range regularNodes {
			res.Data[n] = &pbv2.LinkedGraph{Arcs: map[string]*pbv2.Nodes{}}
			for _, property := range properties {
				if nodes := data[n][property]; len(nodes) > 0 {
					res.Data[n].Arcs[property] = &pbv2.Nodes{
						Nodes: v1pv.MergeTypedNodes(data[n][property]),
					}
				}
			}
		}

		if pi != nil {
			respToken, err := util.EncodeProto(pi)
			if err != nil {
				return nil, err
			}
			res.NextToken = respToken
		}
	}

	// Post-filter the results using language filters.
	globalLangs := arc.Filter["$lang"]
	for n := range res.Data {
		for property := range res.Data[n].Arcs {
			langs := globalLangs
			if bracketFilters, ok := arc.BracketFilters[property]; ok {
				if propLangs, ok := bracketFilters["$lang"]; ok {
					langs = propLangs
				}
			}
			if len(langs) > 0 {
				filtered := filterByLang(res.Data[n].Arcs[property].Nodes, langs)
				if len(filtered) > 0 {
					res.Data[n].Arcs[property].Nodes = filtered
				} else {
					delete(res.Data[n].Arcs, property)
				}
			}
		}
	}

	return res, nil
}

// LinkedPropertyValues is the V2 linked property values API implementation entry point.
func LinkedPropertyValues(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	nodes []string,
	linkedProperty string,
	direction string,
	typeOfFilters []string,
) (*pbv2.NodeResponse, error) {
	if len(typeOfFilters) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "must provide typeOf filters")
	}
	if linkedProperty == "containedInPlace" && direction == util.DirectionIn {
		nodeChildren := make(map[string][]string)
		nodeChildrenSet := make(map[string]map[string]struct{})
		allChildSet := make(map[string]struct{})

		for _, typeOfFilter := range typeOfFilters {
			data, err := placein.GetPlacesIn(
				ctx,
				store,
				nodes,
				typeOfFilter,
			)
			if err != nil {
				return nil, err
			}
			for _, node := range nodes {
				dcids, ok := data[node]
				if !ok || len(dcids) == 0 {
					continue
				}
				if _, exists := nodeChildrenSet[node]; !exists {
					nodeChildrenSet[node] = make(map[string]struct{})
				}
				for _, dcid := range dcids {
					if _, seen := nodeChildrenSet[node][dcid]; !seen {
						nodeChildren[node] = append(nodeChildren[node], dcid)
						nodeChildrenSet[node][dcid] = struct{}{}
						allChildSet[dcid] = struct{}{}
					}
				}
			}
		}

		descendents := make([]string, 0, len(allChildSet))
		for dcid := range allChildSet {
			descendents = append(descendents, dcid)
		}

		if len(descendents) == 0 {
			res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
			for _, node := range nodes {
				res.Data[node] = &pbv2.LinkedGraph{
					Arcs: map[string]*pbv2.Nodes{
						"containedInPlace" + CHAIN: {Nodes: []*pb.EntityInfo{}},
					},
				}
			}
			return res, nil
		}

		nameResp, _, err := v1pv.Fetch(
			ctx,
			store,
			descendents,
			[]string{"name"},
			0,
			"",
			"out",
		)
		if err != nil {
			return nil, err
		}

		res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		for _, node := range nodes {
			children := nodeChildren[node]
			list := make([]*pb.EntityInfo, 0, len(children))
			for _, dcid := range children {
				info := &pb.EntityInfo{Dcid: dcid}
				if v, ok := nameResp[dcid]["name"]; ok {
					if len(v[""]) > 0 {
						info.Name = v[""][0].Value
					}
				}
				list = append(list, info)
			}
			res.Data[node] = &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace" + CHAIN: {Nodes: list},
				},
			}
		}
		return res, nil
	} else if linkedProperty == hierarchy.SpecializationOf &&
		direction == util.DirectionOut &&
		len(typeOfFilters) == 1 &&
		typeOfFilters[0] == hierarchy.StatVarGroup {
		res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		parentSvgs := cachedata.ParentSvgs(ctx)
		for _, node := range nodes {
			res.Data[node] = &pbv2.LinkedGraph{}
			ancestors := hierarchy.GetSVGAncestors(node, parentSvgs)
			if len(ancestors) > 0 {
				res.Data[node].Arcs = map[string]*pbv2.Nodes{
					hierarchy.SpecializationOf + CHAIN: {
						Nodes: ancestors,
					},
				}
			}
		}
		return res, nil
	}
	return nil, status.Errorf(codes.InvalidArgument,
		"Invalid property %s for wildcard '+'", linkedProperty)
}

func filterByLang(nodes []*pb.EntityInfo, langs []string) []*pb.EntityInfo {
	if len(langs) == 0 {
		return nodes
	}
	res := []*pb.EntityInfo{}
	for _, n := range nodes {
		for _, lang := range langs {
			if strings.HasSuffix(n.Value, "@"+strings.ToLower(lang)) {
				res = append(res, n)
				break
			}
		}
	}
	return res
}
