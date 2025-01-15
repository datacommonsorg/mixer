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

	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/util"
)

// RemoteClient encapsulates the Remote client.
type RemoteClient struct {
	metadata   *resource.Metadata
	httpClient *http.Client
}

// NewRemoteClient creates a new RemoteClient.
func NewRemoteClient(metadata *resource.Metadata, httpClient *http.Client) (*RemoteClient, error) {
	if metadata.RemoteMixerDomain == "" || metadata.RemoteMixerAPIKey == "" {
		return nil, fmt.Errorf("error creating remote client")
	}
	return &RemoteClient{metadata, httpClient}, nil
}

func (rc *RemoteClient) Node(req *pbv3.NodeRequest) (*pbv3.NodeResponse, error) {
	resp := &pbv3.NodeResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v3/node", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Observation(req *pbv3.ObservationRequest) (*pbv3.ObservationResponse, error) {
	resp := &pbv3.ObservationResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v3/observation", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) NodeSearch(req *pbv3.NodeSearchRequest) (*pbv3.NodeSearchResponse, error) {
	resp := &pbv3.NodeSearchResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v3/node_search", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (rc *RemoteClient) Resolve(req *pbv3.ResolveRequest) (*pbv3.ResolveResponse, error) {
	resp := &pbv3.ResolveResponse{}
	err := util.FetchRemote(rc.metadata, rc.httpClient, "/v3/resolve", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
