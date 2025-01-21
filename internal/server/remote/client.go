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
	"fmt"
	"net/http"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"

	"github.com/datacommonsorg/mixer/internal/util"
)

// RemoteClient encapsulates a client for a Remote Mixer.
type RemoteClient struct {
	metadata   *resource.Metadata
	httpClient *http.Client
}

// NewRemoteClient creates a new RemoteClient.
func NewRemoteClient(metadata *resource.Metadata) (*RemoteClient, error) {
	if metadata.RemoteMixerDomain == "" || metadata.RemoteMixerAPIKey == "" {
		return nil, fmt.Errorf("error creating remote client: please ensure that the remote mixer domain and API key are set")
	}
	return &RemoteClient{metadata, &http.Client{}}, nil
}

func (rc *RemoteClient) Node(req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	resp := &pbv2.NodeResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v2/node", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Observation(req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v2/observation", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) NodeSearch(req *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	resp := &pbv2.NodeSearchResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v3/node_search", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Resolve(req *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	resp := &pbv2.ResolveResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v2/resolve", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
