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

package mcp

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/agent"
	"google.golang.org/grpc/metadata"
)

// fakeAgentBackend records incoming requests and returns canned responses for unit tests.
type fakeAgentBackend struct {
	lastSurface           string
	lastSearchReq         *agent.SearchIndicatorsRequest
	searchResp            *agent.SearchIndicatorsResponse
	lastVarMetadataReq    *agent.GetVariableMetadataRequest
	varMetadataResp       *agent.GetVariableMetadataResponse
	lastObservationsReq   *agent.GetObservationsRequest
	observationsResp      *agent.GetObservationsResponse
	lastV2ResolveReq      *agent.V2ResolveEntitiesRequest
	v2ResolveResp         *agent.V2ResolveEntitiesResponse
	lastV2SearchReq       *agent.V2SearchIndicatorsRequest
	lastV2ObservationsReq *agent.V2GetObservationsRequest
	v2ObservationsResp    *agent.V2GetObservationsResponse
}

func (f *fakeAgentBackend) recordSurface(ctx context.Context) {
	f.lastSurface = ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(surfaceHeaderKey); len(vals) > 0 {
			f.lastSurface = vals[0]
		}
	}
}

func (f *fakeAgentBackend) SearchIndicators(ctx context.Context, req *agent.SearchIndicatorsRequest) (*agent.SearchIndicatorsResponse, error) {
	f.recordSurface(ctx)
	f.lastSearchReq = req
	if f.searchResp != nil {
		return f.searchResp, nil
	}
	return &agent.SearchIndicatorsResponse{Status: "SUCCESS"}, nil
}

func (f *fakeAgentBackend) GetVariableMetadata(ctx context.Context, req *agent.GetVariableMetadataRequest) (*agent.GetVariableMetadataResponse, error) {
	f.recordSurface(ctx)
	f.lastVarMetadataReq = req
	if f.varMetadataResp != nil {
		return f.varMetadataResp, nil
	}
	return &agent.GetVariableMetadataResponse{Status: "SUCCESS"}, nil
}

func (f *fakeAgentBackend) GetObservations(ctx context.Context, req *agent.GetObservationsRequest) (*agent.GetObservationsResponse, error) {
	f.recordSurface(ctx)
	f.lastObservationsReq = req
	if f.observationsResp != nil {
		return f.observationsResp, nil
	}
	return &agent.GetObservationsResponse{
		Variable: &agent.ObservationNode{Dcid: req.VariableDcid, Name: "Test Variable"},
	}, nil
}

func (f *fakeAgentBackend) V2ResolveEntities(ctx context.Context, req *agent.V2ResolveEntitiesRequest) (*agent.V2ResolveEntitiesResponse, error) {
	f.recordSurface(ctx)
	f.lastV2ResolveReq = req
	if f.v2ResolveResp != nil {
		return f.v2ResolveResp, nil
	}
	return &agent.V2ResolveEntitiesResponse{}, nil
}

func (f *fakeAgentBackend) V2SearchIndicators(ctx context.Context, req *agent.V2SearchIndicatorsRequest) (*agent.V2SearchIndicatorsResponse, error) {
	f.recordSurface(ctx)
	f.lastV2SearchReq = req
	return &agent.V2SearchIndicatorsResponse{}, nil
}

func (f *fakeAgentBackend) V2GetObservations(ctx context.Context, req *agent.V2GetObservationsRequest) (*agent.V2GetObservationsResponse, error) {
	f.recordSurface(ctx)
	f.lastV2ObservationsReq = req
	if f.v2ObservationsResp != nil {
		return f.v2ObservationsResp, nil
	}
	return &agent.V2GetObservationsResponse{}, nil
}
