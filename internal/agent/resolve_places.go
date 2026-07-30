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
	"google.golang.org/protobuf/types/known/structpb"
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
	req *pbv2.ResolvePlacesRequest,
) (*pbv2.ResolvePlacesResponse, error) {
	if err := validateResolvePlacesRequest(req); err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: ResolvePlaces")
	slog.Info("ResolvePlaces started", "places", req.GetPlaces(), "target", req.GetTarget())

	if len(req.GetPlaces()) == 0 {
		return emptyResolvePlacesResponse(), nil
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:    req.GetPlaces(),
		Resolver: ResolverPlace,
		Target:   req.GetTarget(),
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

	table, err := buildResolvedPlacesTable(entries)
	if err != nil {
		return nil, err
	}

	return &pbv2.ResolvePlacesResponse{
		Status:         StatusSuccess,
		ResolvedPlaces: table,
	}, nil
}

func validateResolvePlacesRequest(req *pbv2.ResolvePlacesRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if req.GetTarget() != "" && !slices.Contains(validTargets, req.GetTarget()) {
		return status.Errorf(codes.InvalidArgument, "invalid target %q, must be one of %v", req.GetTarget(), validTargets)
	}
	return nil
}

func emptyResolvePlacesResponse() *pbv2.ResolvePlacesResponse {
	return &pbv2.ResolvePlacesResponse{
		Status: StatusSuccess,
		ResolvedPlaces: &pbv2.Table{
			Columns: []string{colQuery, colDcid, colName, colTypeOf},
			Rows:    []*structpb.ListValue{},
		},
	}
}

func parseResolvedPlaceEntries(entities []*pbv2.ResolveResponse_Entity) ([]*resolvedPlaceEntry, []string, map[string][]*resolvedPlaceEntry) {
	var entries []*resolvedPlaceEntry
	var dcidsToEnrich []string
	dcidToEntries := make(map[string][]*resolvedPlaceEntry)

	for _, entity := range entities {
		query := entity.GetNode()
		for _, candidate := range entity.GetCandidates() {
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

			if !slices.Contains(dcidsToEnrich, dcid) {
				dcidsToEnrich = append(dcidsToEnrich, dcid)
			}
			dcidToEntries[dcid] = append(dcidToEntries[dcid], entry)
		}
	}
	return entries, dcidsToEnrich, dcidToEntries
}

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
	nodeResp, err := s.mixer.V2Node(ctx, nodeReq)
	if err != nil || nodeResp == nil || nodeResp.GetData() == nil {
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

func buildResolvedPlacesTable(entries []*resolvedPlaceEntry) (*pbv2.Table, error) {
	var rows []*structpb.ListValue
	for _, entry := range entries {
		typeList := util.ToStringListValue(entry.typeOf)
		row, err := structpb.NewList([]any{
			entry.query,
			entry.dcid,
			entry.name,
			typeList.AsSlice(),
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to build row for place %s: %v", entry.dcid, err)
		}
		rows = append(rows, row)
	}

	return &pbv2.Table{
		Columns: []string{colQuery, colDcid, colName, colTypeOf},
		Rows:    rows,
	}, nil
}
