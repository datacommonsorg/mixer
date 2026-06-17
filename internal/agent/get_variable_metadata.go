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

package agent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const wildcardPropertyQuery = "->*"

var obsMetadataSelect = []string{"variable", "entity", "facet"}

// GetVariableMetadata assesses Statistical Variables by retrieving their definitions,
// temporal/entity coverage, and source provenance descriptions.
func (s *Service) GetVariableMetadata(
	ctx context.Context,
	req *pbv2.GetVariableMetadataRequest,
) (*pbv2.GetVariableMetadataResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	defer util.TimeTrack(time.Now(), "Agent: GetVariableMetadata")
	slog.Info("GetVariableMetadata started", "variablesCount", len(req.GetVariableDcids()), "entitiesCount", len(req.GetEntityDcids()))

	if len(req.GetVariableDcids()) == 0 {
		return &pbv2.GetVariableMetadataResponse{
			Status:    StatusSuccess,
			Variables: make(map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata),
		}, nil
	}

	variables := initVariableMetadata(req.GetVariableDcids())

	if err := s.fetchCoreMetadata(ctx, req, variables); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch core variable metadata: %v", err)
	}

	if err := s.hydrateProvenanceMetadata(ctx, variables); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to hydrate provenance metadata: %v", err)
	}

	return &pbv2.GetVariableMetadataResponse{
		Status:    StatusSuccess,
		Variables: variables,
	}, nil
}

// initVariableMetadata initializes the map of VariableMetadata entries.
func initVariableMetadata(dcids []string) map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata {
	res := make(map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata)
	for _, vDcid := range dcids {
		res[vDcid] = &pbv2.GetVariableMetadataResponse_VariableMetadata{
			Dcid:            vDcid,
			Properties:      make(map[string]*pbv2.PropertyValues),
			Provenances:     make(map[string]*pbv2.GetVariableMetadataResponse_ProvenanceMetadata),
			PerEntityFacets: make(map[string]*pb.Facet),
		}
	}
	return res
}

// fetchCoreMetadata concurrently retrieves general properties, summaries, and observation facets.
// Note on Caching: We execute V2Node and V2BulkVariableInfo calls individually per variable via
// parallel goroutines rather than a single batch call. Because the underlying Redis caching layer
// builds cache keys from the exact requested node slice, single-node lookups maximize cache-hit rates across requests.
func (s *Service) fetchCoreMetadata(
	ctx context.Context,
	req *pbv2.GetVariableMetadataRequest,
	variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata,
) error {
	g, gCtx := errgroup.WithContext(ctx)
	var mu sync.Mutex

	for _, vDcid := range req.GetVariableDcids() {
		dcid := vDcid
		g.Go(func() error {
			return s.fetchVariableProperties(gCtx, dcid, variables, &mu)
		})
		g.Go(func() error {
			return s.fetchVariableSummary(gCtx, dcid, variables, &mu)
		})
	}

	if len(req.GetEntityDcids()) > 0 {
		g.Go(func() error {
			return s.fetchPerEntityFacets(gCtx, req, variables, &mu)
		})
	}

	return g.Wait()
}

// fetchVariableProperties fetches outbound properties for a single Statistical Variable.
func (s *Service) fetchVariableProperties(
	ctx context.Context, dcid string,
	variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata,
	mu *sync.Mutex,
) error {
	req := &pbv2.NodeRequest{Nodes: []string{dcid}, Property: wildcardPropertyQuery}
	resp, err := s.mixer.V2Node(ctx, req)
	if err != nil {
		return err
	}
	if resp != nil && resp.GetData() != nil {
		if graph, ok := resp.GetData()[dcid]; ok && graph != nil {
			props := toPropertyValuesMap(graph)
			mu.Lock()
			variables[dcid].Properties = props
			mu.Unlock()
		}
	}
	return nil
}

// fetchVariableSummary retrieves summary information for a single Statistical Variable.
func (s *Service) fetchVariableSummary(
	ctx context.Context, dcid string,
	variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata,
	mu *sync.Mutex,
) error {
	req := &pbv1.BulkVariableInfoRequest{Nodes: []string{dcid}}
	resp, err := s.mixer.V2BulkVariableInfo(ctx, req)
	if err != nil {
		return err
	}
	if resp != nil {
		for _, info := range resp.GetData() {
			if info.GetNode() == dcid && info.GetInfo() != nil {
				mu.Lock()
				variables[dcid].StatVarSummary = info.GetInfo()
				mu.Unlock()
			}
		}
	}
	return nil
}

// fetchPerEntityFacets retrieves observation facets across target variables and entities.
func (s *Service) fetchPerEntityFacets(
	ctx context.Context,
	req *pbv2.GetVariableMetadataRequest,
	variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata,
	mu *sync.Mutex,
) error {
	obsReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: req.GetVariableDcids()},
		Entity:   &pbv2.DcidOrExpression{Dcids: req.GetEntityDcids()},
		Select:   obsMetadataSelect,
	}
	obsResp, err := s.mixer.V2Observation(ctx, obsReq)
	if err != nil {
		return err
	}
	if obsResp != nil && obsResp.GetFacets() != nil {
		mu.Lock()
		for fID, fObj := range obsResp.GetFacets() {
			for _, vMeta := range variables {
				vMeta.PerEntityFacets[fID] = fObj
			}
		}
		mu.Unlock()
	}
	return nil
}

// hydrateProvenanceMetadata collects unique provenance IDs and hydrates their descriptive properties.
func (s *Service) hydrateProvenanceMetadata(
	ctx context.Context,
	variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata,
) error {
	provIDs := collectProvenanceIDs(variables)
	if len(provIDs) == 0 {
		return nil
	}
	slog.Info("Hydrating provenance descriptions", "provenancesCount", len(provIDs))

	g, gCtx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	provenancesMap := make(map[string]*pbv2.GetVariableMetadataResponse_ProvenanceMetadata)

	for _, pID := range provIDs {
		provID := pID
		g.Go(func() error {
			return s.fetchProvenanceMetadata(gCtx, provID, provenancesMap, &mu)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for _, vMeta := range variables {
		vMeta.Provenances = make(map[string]*pbv2.GetVariableMetadataResponse_ProvenanceMetadata)
		for pDcid, pMeta := range provenancesMap {
			vMeta.Provenances[pDcid] = pMeta
		}
	}
	return nil
}

// collectProvenanceIDs gathers all unique provenance identifiers across summaries and facets.
func collectProvenanceIDs(variables map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata) []string {
	seen := make(map[string]struct{})
	for _, vMeta := range variables {
		if vMeta.StatVarSummary != nil {
			for provID := range vMeta.StatVarSummary.GetProvenanceSummary() {
				if provID != "" {
					seen[provID] = struct{}{}
				}
			}
		}
		for _, fObj := range vMeta.PerEntityFacets {
			if fObj != nil && fObj.GetProvenanceId() != "" {
				seen[fObj.GetProvenanceId()] = struct{}{}
			}
		}
	}

	var ids []string
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// fetchProvenanceMetadata fetches descriptive properties for a single provenance entity.
func (s *Service) fetchProvenanceMetadata(
	ctx context.Context, pDcid string,
	provenancesMap map[string]*pbv2.GetVariableMetadataResponse_ProvenanceMetadata,
	mu *sync.Mutex,
) error {
	req := &pbv2.NodeRequest{Nodes: []string{pDcid}, Property: wildcardPropertyQuery}
	resp, err := s.mixer.V2Node(ctx, req)
	if err != nil {
		return err
	}
	if resp != nil && resp.GetData() != nil {
		if graph, ok := resp.GetData()[pDcid]; ok && graph != nil {
			props := toPropertyValuesMap(graph)
			mu.Lock()
			provenancesMap[pDcid] = &pbv2.GetVariableMetadataResponse_ProvenanceMetadata{
				Dcid:       pDcid,
				Properties: props,
			}
			mu.Unlock()
		}
	}
	return nil
}

// toPropertyValuesMap converts a LinkedGraph into a map of PropertyValues lists.
func toPropertyValuesMap(graph *pbv2.LinkedGraph) map[string]*pbv2.PropertyValues {
	if graph == nil || graph.GetArcs() == nil {
		return make(map[string]*pbv2.PropertyValues)
	}

	res := make(map[string]*pbv2.PropertyValues)
	for prop, nodesArc := range graph.GetArcs() {
		if nodesArc == nil || len(nodesArc.GetNodes()) == 0 {
			continue
		}

		var pvList []*pbv2.PropertyValue
		seen := make(map[string]struct{})
		for _, n := range nodesArc.GetNodes() {
			if n == nil {
				continue
			}
			val := n.GetValue()
			dcid := n.GetDcid()
			name := n.GetName()

			key := val + "|" + dcid + "|" + name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			pv := &pbv2.PropertyValue{}
			if val != "" {
				pv.Value = val
			}
			if dcid != "" {
				pv.Dcid = dcid
			}
			if name != "" {
				pv.Name = name
			}
			pvList = append(pvList, pv)
		}
		res[prop] = &pbv2.PropertyValues{Values: pvList}
	}
	return res
}
