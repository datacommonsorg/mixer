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
	"slices"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolvedPlaceEntry holds intermediate candidate resolution data.
type resolvedPlaceEntry struct {
	query  string
	dcid   string
	name   string
	typeOf []string
}

// ResolvePlaces resolves place name queries to canonical place DCIDs, names, and place types.
func (s *Service) ResolvePlaces(
	ctx context.Context,
	req *ResolvePlacesRequest,
) (*ResolvePlacesResponse, error) {
	if err := validateResolvePlacesRequest(req); err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: ResolvePlaces")
	slog.Info("ResolvePlaces started", "places", req.Places, "target", req.Target)

	if len(req.Places) == 0 {
		return emptyResolvePlacesResponse(), nil
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:    req.Places,
		Resolver: ResolverPlace,
		Target:   req.Target,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve places: %v", err)
	}
	if resp == nil {
		return nil, status.Error(codes.Internal, "received nil response from V2Resolve")
	}

	entries, dcidsToEnrich, dcidToEntries := parseResolvedPlaceEntries(resp.GetEntities())

	s.enrichPlaceEntries(ctx, dcidsToEnrich, dcidToEntries)

	table := buildResolvedPlacesTable(entries)

	slog.Info("ResolvePlaces completed", "placesCount", len(req.Places), "rowsCount", len(table.Rows))

	return &ResolvePlacesResponse{
		Status:         StatusSuccess,
		ResolvedPlaces: table,
	}, nil
}

// validateResolvePlacesRequest checks the validity of ResolvePlacesRequest parameters.
func validateResolvePlacesRequest(req *ResolvePlacesRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if req.Target != "" && !slices.Contains(validTargets, req.Target) {
		return status.Errorf(codes.InvalidArgument, "invalid target %q, must be one of %v", req.Target, validTargets)
	}
	return nil
}

// emptyResolvePlacesResponse returns a ResolvePlacesResponse with empty rows.
func emptyResolvePlacesResponse() *ResolvePlacesResponse {
	return &ResolvePlacesResponse{
		Status: StatusSuccess,
		ResolvedPlaces: &Table{
			Columns: []string{colQuery, colDcid, colName, colTypeOf},
			Rows:    [][]any{},
		},
	}
}

// parseResolvedPlaceEntries extracts place candidate entries and unique DCIDs to enrich.
func parseResolvedPlaceEntries(entities []*pbv2.ResolveResponse_Entity) ([]*resolvedPlaceEntry, []string, map[string][]*resolvedPlaceEntry) {
	var entries []*resolvedPlaceEntry
	var dcidsToEnrich []string
	dcidToEntries := make(map[string][]*resolvedPlaceEntry)

	for _, entity := range entities {
		query := entity.GetNode()
		candidates := entity.GetCandidates()
		addedCount := 0

		for _, candidate := range candidates {
			dcid := candidate.GetDcid()
			if dcid == "" {
				continue
			}
			entry := &resolvedPlaceEntry{
				query:  query,
				dcid:   dcid,
				name:   candidate.GetName(),
				typeOf: candidate.GetTypeOf(),
			}
			entries = append(entries, entry)
			addedCount++

			if !slices.Contains(dcidsToEnrich, dcid) {
				dcidsToEnrich = append(dcidsToEnrich, dcid)
			}
			dcidToEntries[dcid] = append(dcidToEntries[dcid], entry)
		}

		// If no valid candidates were resolved for this query, append an empty placeholder entry.
		// This guarantees 1:1 row alignment between input place queries and output table rows,
		// allowing conversational assistant callers to determine which requested queries failed to resolve.
		if addedCount == 0 {
			entries = append(entries, &resolvedPlaceEntry{
				query:  query,
				dcid:   "",
				name:   "",
				typeOf: []string{},
			})
		}
	}
	return entries, dcidsToEnrich, dcidToEntries
}

// enrichPlaceEntries fetches canonical names and types for resolved place DCIDs via V2Node.
func (s *Service) enrichPlaceEntries(
	ctx context.Context,
	dcidsToEnrich []string,
	dcidToEntries map[string][]*resolvedPlaceEntry,
) {
	if len(dcidsToEnrich) == 0 {
		return
	}
	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcidsToEnrich,
		Property: nodePropertiesQuery,
	}
	nodeResp, err := s.fetchAllNodes(ctx, nodeReq)
	if err != nil {
		slog.Warn("Failed to enrich place entries via V2Node", "error", err, "dcidsCount", len(dcidsToEnrich))
		return
	}
	if nodeResp == nil || nodeResp.GetData() == nil {
		return
	}
	for dcid, targetEntries := range dcidToEntries {
		if nodeData, ok := nodeResp.GetData()[dcid]; ok {
			name := getPropValue(nodeData, "name")
			types := getPropDcids(nodeData, "typeOf")
			for _, entry := range targetEntries {
				if name != "" {
					entry.name = name
				}
				if len(types) > 0 {
					entry.typeOf = types
				}
			}
		}
	}
}

// buildResolvedPlacesTable formats resolved place entries into a tabular Table.
func buildResolvedPlacesTable(entries []*resolvedPlaceEntry) *Table {
	var rows [][]any
	for _, entry := range entries {
		rows = append(rows, []any{
			entry.query,
			entry.dcid,
			entry.name,
			util.NonNilStringSlice(entry.typeOf),
		})
	}

	return &Table{
		Columns: []string{colQuery, colDcid, colName, colTypeOf},
		Rows:    rows,
	}
}
