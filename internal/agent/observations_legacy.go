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
	"fmt"
	"log"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeMetadata struct {
	dcid   string
	name   string
	typeOf []string
}

// getObservationsLegacy encapsulates the legacy V2Observation execution path for backward compatibility.
//
//nolint:staticcheck // Legacy path accesses deprecated fields for backward compatibility
func (s *Service) getObservationsLegacy(
	ctx context.Context,
	in *pbv2.GetObservationsRequest,
) (*pbv2.GetObservationsResponse, error) {
	if in.GetPlaceDcid() == "" {
		return nil, status.Error(codes.InvalidArgument, "place_dcid must be specified")
	}

	filter, err := parseDateFilter(in.GetDate(), in.GetDateRangeStart(), in.GetDateRangeEnd())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Fetch Core Observation Data via legacy V2Observation
	obsReq := buildObservationRequest(in, filter.dateType)
	obsResp, err := s.mixer.V2Observation(ctx, obsReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch V2Observation: %v", err)
	}

	// Metadata Enrichment (Single V2Node Lookup)
	metadata, err := s.enrichMetadata(ctx, in.GetVariableDcid(), in.GetPlaceDcid(), obsResp)
	if err != nil {
		log.Printf("Agent GetObservations: failed to enrich metadata: %v", err)
		if metadata == nil {
			metadata = make(map[string]*nodeMetadata)
		}
	}

	// Primary Source Selection (Ranking) & Date Filtering
	var variableData *pbv2.VariableObservation
	if obsResp != nil && obsResp.GetByVariable() != nil {
		variableData = obsResp.GetByVariable()[in.GetVariableDcid()]
	}
	sourceResult := selectPrimarySource(variableData, in.GetSourceOverride(), filter)

	// Assemble Final Response
	return s.buildFinalResponse(in, obsResp, metadata, sourceResult)
}

// buildObservationRequest constructs the V2 Observation request from the agent request.
//
//nolint:staticcheck // Legacy request builder uses deprecated place fields
func buildObservationRequest(in *pbv2.GetObservationsRequest, dateType string) *pbv2.ObservationRequest {
	var entity pbv2.DcidOrExpression
	if in.GetChildPlaceType() != "" {
		entity.Expression = fmt.Sprintf("%s<-containedInPlace+{typeOf:%s}", in.GetPlaceDcid(), in.GetChildPlaceType())
	} else {
		entity.Dcids = []string{in.GetPlaceDcid()}
	}

	dateRequest := ""
	if dateType == dateTypeLatest {
		dateRequest = "LATEST"
	}

	obsReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{
			Dcids: []string{in.GetVariableDcid()},
		},
		Entity: &entity,
		Date:   dateRequest,
		Select: []string{"variable", "entity", "date", "value", "facet"},
	}

	if in.GetSourceOverride() != "" {
		obsReq.Filter = &pbv2.FacetFilter{
			FacetIds: []string{in.GetSourceOverride()},
		}
	}

	return obsReq
}

// enrichMetadata performs a single V2Node call to fetch names and types for all entities.
func (s *Service) enrichMetadata(
	ctx context.Context,
	variableDcid string,
	parentDcid string,
	obsResp *pbv2.ObservationResponse,
) (map[string]*nodeMetadata, error) {
	dcidsSet := make(map[string]bool)
	dcidsSet[variableDcid] = true
	dcidsSet[parentDcid] = true

	if obsResp != nil && obsResp.GetByVariable() != nil {
		if varData, ok := obsResp.GetByVariable()[variableDcid]; ok {
			for placeDcid := range varData.GetByEntity() {
				dcidsSet[placeDcid] = true
			}
		}
	}

	var allDcids []string
	for dcid := range dcidsSet {
		allDcids = append(allDcids, dcid)
	}
	sort.Strings(allDcids)

	nodeReq := &pbv2.NodeRequest{
		Nodes:    allDcids,
		Property: nodePropertiesQuery,
	}

	nodeResp, err := s.mixer.V2Node(ctx, nodeReq)
	if err != nil {
		return nil, err
	}

	metadataMap := make(map[string]*nodeMetadata)
	for _, dcid := range allDcids {
		meta := &nodeMetadata{dcid: dcid}
		if nodeResp != nil && nodeResp.GetData() != nil {
			if graph, ok := nodeResp.GetData()[dcid]; ok && graph.GetArcs() != nil {
				if names, ok := graph.GetArcs()["name"]; ok && len(names.GetNodes()) > 0 {
					meta.name = names.GetNodes()[0].GetValue()
				}
				if types, ok := graph.GetArcs()["typeOf"]; ok {
					var typeOf []string
					for _, typeNode := range types.GetNodes() {
						typeOf = append(typeOf, typeNode.GetDcid())
					}
					meta.typeOf = typeOf
				}
			}
		}
		metadataMap[dcid] = meta
	}

	return metadataMap, nil
}

// buildFinalResponse compiles all processed data and metadata into the final response protobuf.
//
//nolint:staticcheck // Legacy response builder populates deprecated response fields
func (s *Service) buildFinalResponse(
	in *pbv2.GetObservationsRequest,
	obsResp *pbv2.ObservationResponse,
	metadata map[string]*nodeMetadata,
	sourceResult *sourceProcessingResult,
) (*pbv2.GetObservationsResponse, error) {
	resp := &pbv2.GetObservationsResponse{
		ChildPlaceType: in.GetChildPlaceType(),
	}

	resp.Variable = toResponseNode(in.GetVariableDcid(), metadata)

	if in.GetChildPlaceType() != "" {
		resp.ResolvedParentPlace = toResponseNode(in.GetPlaceDcid(), metadata)
	}

	primarySourceID := sourceResult.primarySourceID
	if primarySourceID != "" && obsResp != nil && obsResp.GetFacets() != nil {
		if facet, ok := obsResp.GetFacets()[primarySourceID]; ok {
			resp.SourceMetadata = toFacetMetadata(primarySourceID, facet)
		}
	}
	if resp.SourceMetadata == nil {
		resp.SourceMetadata = &pbv2.GetObservationsResponse_FacetMetadata{
			SourceId: "unknown",
		}
	}

	if obsResp != nil && obsResp.GetFacets() != nil {
		var altSourceIDs []string
		for altSourceID := range sourceResult.alternativeSourceCounts {
			altSourceIDs = append(altSourceIDs, altSourceID)
		}
		sort.Strings(altSourceIDs)

		for _, altSourceID := range altSourceIDs {
			count := sourceResult.alternativeSourceCounts[altSourceID]
			if facet, ok := obsResp.GetFacets()[altSourceID]; ok {
				alt := &pbv2.GetObservationsResponse_AlternativeSource{
					SourceMetadata: toFacetMetadata(altSourceID, facet),
				}
				if len(sourceResult.processedDataByPlace) > 1 {
					c := int32(count)
					alt.PlacesFoundCount = &c
				}
				resp.AlternativeSources = append(resp.AlternativeSources, alt)
			}
		}
	}

	var placesList []string
	if obsResp != nil && obsResp.GetByVariable() != nil {
		if varData, ok := obsResp.GetByVariable()[in.GetVariableDcid()]; ok {
			for placeDcid := range varData.GetByEntity() {
				placesList = append(placesList, placeDcid)
			}
		}
	}
	sort.Strings(placesList)

	for _, placeDcid := range placesList {
		placeObs := &pbv2.GetObservationsResponse_PlaceObservation{
			Place: toResponseNode(placeDcid, metadata),
		}

		if processed, ok := sourceResult.processedDataByPlace[placeDcid]; ok {
			for _, pt := range processed.observations {
				placeObs.TimeSeries = append(placeObs.TimeSeries, &pbv2.GetObservationsResponse_TimeSeriesPoint{
					Date:  pt.GetDate(),
					Value: pt.GetValue(),
				})
			}
		}
		resp.PlaceObservations = append(resp.PlaceObservations, placeObs)
	}

	return resp, nil
}

// toFacetMetadata maps core protobuf Facet into internal GetObservationsResponse_FacetMetadata message.
func toFacetMetadata(sourceID string, facet *pb.Facet) *pbv2.GetObservationsResponse_FacetMetadata {
	if facet == nil {
		return &pbv2.GetObservationsResponse_FacetMetadata{
			SourceId: sourceID,
		}
	}
	return &pbv2.GetObservationsResponse_FacetMetadata{
		SourceId:          sourceID,
		ImportName:        facet.GetImportName(),
		MeasurementMethod: facet.GetMeasurementMethod(),
		ObservationPeriod: facet.GetObservationPeriod(),
		ProvenanceUrl:     facet.GetProvenanceUrl(),
		Unit:              facet.GetUnit(),
	}
}

// toResponseNode constructs a response Node, always populating the DCID,
// and conditionally enriching it with name and type if metadata is available.
func toResponseNode(dcid string, metadata map[string]*nodeMetadata) *pbv2.GetObservationsResponse_Node {
	node := &pbv2.GetObservationsResponse_Node{
		Dcid: dcid,
	}
	if meta, ok := metadata[dcid]; ok {
		node.Name = meta.name
		node.TypeOf = meta.typeOf
	}
	return node
}
