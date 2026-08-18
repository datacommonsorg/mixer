// Copyright 2026 Google LLC
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

package dispatcher

import (
	"fmt"
	"log/slog"
	"slices"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
)

const (
	unresolvedNodeType = "Thing"
	nodeInfoProperty   = "->[name, typeOf]"
	predicateName      = "name"
	predicateTypeOf    = "typeOf"
)

type entityInfo struct {
	name  string
	types []string
}

// UnresolvedNodeProcessor post-processes NodeResponses to resolve and hydrate
// properties (name, types) for unresolved nodes using the provided datasource.
type UnresolvedNodeProcessor struct {
	source datasource.DataSource
}

// NewUnresolvedNodeProcessor creates a new UnresolvedNodeProcessor.
func NewUnresolvedNodeProcessor(source datasource.DataSource) *UnresolvedNodeProcessor {
	return &UnresolvedNodeProcessor{
		source: source,
	}
}

// PreProcess is a no-op for UnresolvedNodeProcessor.
func (p *UnresolvedNodeProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	return Continue, nil
}

// PostProcess resolves unresolved nodes in the NodeResponse by fetching their
// properties from the configured datasource.
func (p *UnresolvedNodeProcessor) PostProcess(rc *RequestContext) (Outcome, error) {
	if rc.Type != TypeNode || p.source == nil || rc.CurrentResponse == nil {
		return Continue, nil
	}

	resp, ok := rc.CurrentResponse.(*pbv2.NodeResponse)
	if !ok {
		slog.Error("UnresolvedNodeProcessor: failed to cast response to NodeResponse", "type", rc.Type)
		return Continue, fmt.Errorf("failed to cast response to NodeResponse")
	}

	unresolvedDcids := collectUnresolvedDCIDs(resp)
	if len(unresolvedDcids) == 0 {
		return Continue, nil
	}

	slog.Info("UnresolvedNodeProcessor: fetching properties for unresolved nodes", "count", len(unresolvedDcids))

	req := &pbv2.NodeRequest{
		Nodes:    unresolvedDcids,
		Property: nodeInfoProperty,
	}

	sourceResp, err := datasource.NodeFetchAll(rc.Context, p.source, req, datasources.DefaultPageSize)
	if err != nil {
		slog.Warn("UnresolvedNodeProcessor: failed to fetch unresolved node properties", "error", err)
		return Continue, nil
	}

	infoByDcid := extractEntityInfo(sourceResp)
	slog.Info("UnresolvedNodeProcessor: extracted properties for unresolved nodes", "count", len(infoByDcid))
	hydrateNodeResponse(resp, infoByDcid)

	return Continue, nil
}

// collectUnresolvedDCIDs gathers unique DCIDs of all unresolved entities in the response.
func collectUnresolvedDCIDs(resp *pbv2.NodeResponse) []string {
	seen := make(map[string]struct{})
	var dcids []string

	for _, graph := range resp.GetData() {
		for _, arc := range graph.GetArcs() {
			for _, node := range arc.GetNodes() {
				if isUnresolved(node) {
					dcid := node.GetDcid()
					if _, exists := seen[dcid]; !exists {
						seen[dcid] = struct{}{}
						dcids = append(dcids, dcid)
					}
				}
			}
		}
	}

	// Sort DCIDs for deterministic request order and cache stability.
	slices.Sort(dcids)
	return dcids
}

// isUnresolved checks whether an entity node is an unresolved reference.
// NOTE: Local data sources (e.g. Spanner) emit dangling foreign-key nodes with
// Types = ["Thing"] and empty Name. We use this heuristic instead of explicit
// context attributes to keep data source interfaces simple and avoid extra wiring.
func isUnresolved(entity *pb.EntityInfo) bool {
	return len(entity.GetTypes()) == 1 &&
		entity.GetTypes()[0] == unresolvedNodeType &&
		entity.GetName() == "" &&
		entity.GetDcid() != "" &&
		entity.GetValue() == ""
}

// extractEntityInfo extracts the name and types for each DCID from a NodeResponse.
func extractEntityInfo(resp *pbv2.NodeResponse) map[string]*entityInfo {
	infoByDcid := make(map[string]*entityInfo)

	for dcid, graph := range resp.GetData() {
		info := &entityInfo{}

		if nameArc, ok := graph.GetArcs()[predicateName]; ok {
			for _, node := range nameArc.GetNodes() {
				if val := node.GetValue(); val != "" {
					info.name = val
					break
				}
				if name := node.GetName(); name != "" {
					info.name = name
					break
				}
			}
		}

		if typeArc, ok := graph.GetArcs()[predicateTypeOf]; ok {
			for _, node := range typeArc.GetNodes() {
				if typeDcid := node.GetDcid(); typeDcid != "" {
					info.types = append(info.types, typeDcid)
				} else if val := node.GetValue(); val != "" {
					info.types = append(info.types, val)
				}
			}
		}

		infoByDcid[dcid] = info
	}

	return infoByDcid
}

// hydrateNodeResponse enriches unresolved entities in the response with their fetched name and types.
func hydrateNodeResponse(resp *pbv2.NodeResponse, infoByDcid map[string]*entityInfo) {
	for _, graph := range resp.GetData() {
		for _, arc := range graph.GetArcs() {
			for _, node := range arc.GetNodes() {
				info, ok := infoByDcid[node.GetDcid()]
				if !ok {
					continue
				}

				if info.name != "" {
					node.Name = info.name
				}
				if len(info.types) > 0 {
					node.Types = slices.Clone(info.types)
				}
			}
		}
	}
}
