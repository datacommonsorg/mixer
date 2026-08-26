// Copyright 2024 Google LLC
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

// A Remote client wrapper.
package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"

	"github.com/datacommonsorg/mixer/internal/util"
)

// RemoteClient encapsulates a client for a Remote Mixer.
type RemoteClient struct {
	metadata   *resource.Metadata
	httpClient *http.Client
	id         string
}

// NewRemoteClient creates a new RemoteClient.
func NewRemoteClient(metadata *resource.Metadata) (*RemoteClient, error) {
	if metadata.RemoteMixerDomain == "" || metadata.RemoteMixerAPIKey == "" {
		return nil, fmt.Errorf("error creating remote client: please ensure that the remote mixer domain and API key are set")
	}
	return &RemoteClient{
		metadata:   metadata,
		httpClient: &http.Client{},
		id:         metadata.RemoteMixerDomain,
	}, nil
}

func (rc *RemoteClient) Node(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	err := updateNodeRequestNextToken(req, rc.id)
	if err != nil {
		return nil, err
	}

	resp := &pbv2.NodeResponse{}
	err = util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/node", req, resp)
	if err != nil {
		return nil, err
	}

	err = updateNodeResponseNextToken(resp, rc.id)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (rc *RemoteClient) Observation(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/observation", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) NodeSearch(ctx context.Context, req *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	resp := &pbv2.NodeSearchResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v3/node_search", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Resolve(ctx context.Context, req *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	resp := &pbv2.ResolveResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/resolve", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Sparql(ctx context.Context, req *pb.SparqlRequest) (*pb.QueryResponse, error) {
	resp := &pb.QueryResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/sparql", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Event(ctx context.Context, req *pbv2.EventRequest) (*pbv2.EventResponse, error) {
	resp := &pbv2.EventResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/event", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) BulkVariableInfo(ctx context.Context, req *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
	resp := &pbv1.BulkVariableInfoResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/bulk/info/variable", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) BulkVariableGroupInfo(ctx context.Context, req *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error) {
	resp := &pbv1.BulkVariableGroupInfoResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/bulk/info/variable-group", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) FilterStatVarsByEntity(ctx context.Context, req *pb.FilterStatVarsByEntityRequest) (*pb.FilterStatVarsByEntityResponse, error) {
	resp := &pb.FilterStatVarsByEntityResponse{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/variable/filter", req, resp)
	if err != nil {
		slog.Error("Failed to fetch remote variable filter", "error", err)
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	resp := &sdmxpb.SdmxDataResult{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/internal/sdmx/data", req, resp)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// Suppress non-cancellation errors from remote mixer so that upstream failures
		// (e.g. 400 constraint mismatch, 404, or 500) do not cancel concurrent local data sources.
		slog.Warn("RemoteClient: failed to fetch remote SDMX data, ignoring remote error",
			"error", err,
			"path", "/v2/internal/sdmx/data",
		)
		return &sdmxpb.SdmxDataResult{}, nil
	}
	return resp, nil
}

func (rc *RemoteClient) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	resp := &sdmxpb.SdmxAvailabilityResult{}
	err := util.FetchRemote(ctx, rc.metadata, rc.httpClient, "/v2/internal/sdmx/availability", req, resp)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// Suppress non-cancellation errors from remote mixer so that upstream failures
		// (e.g. 400 constraint mismatch, 404, or 500) do not cancel concurrent local data sources.
		slog.Warn("RemoteClient: failed to fetch remote SDMX availability, ignoring remote error",
			"error", err,
			"path", "/v2/internal/sdmx/availability",
		)
		return &sdmxpb.SdmxAvailabilityResult{}, nil
	}
	return resp, nil
}
