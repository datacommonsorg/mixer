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

// Package server is the main server
package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/datacommonsorg/mixer/internal/merger"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

func fetchRemote(
	metadata *resource.Metadata,
	httpClient *http.Client,
	apiPath string,
	in proto.Message,
	out proto.Message,
) error {
	url := metadata.RemoteMixerURL + apiPath
	jsonValue, err := protojson.Marshal(in)
	if err != nil {
		return err
	}
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-API-Key", metadata.RemoteMixerAPIKey)
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// Read response body
	var responseBodyBytes []byte
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("remote mixer response not ok: %s", response.Status)
	}
	responseBodyBytes, err = io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	// Convert response body to string
	return protojson.Unmarshal(responseBodyBytes, out)
}

// V2Resolve implements API for mixer.V2Resolve.
func (s *Server) V2Resolve(
	ctx context.Context, in *pbv2.ResolveRequest,
) (*pbv2.ResolveResponse, error) {
	resp, err := V2ResolveCore(ctx, s.store, s.mapsClient, in)
	if err != nil {
		return nil, err
	}
	if s.metadata.RemoteMixerURL != "" {
		remoteResp := &pbv2.ResolveResponse{}
		err := fetchRemote(s.metadata, s.httpClient, "/v2/resolve", in, remoteResp)
		if err != nil {
			return nil, err
		}
		return merger.MergeResolve(resp, remoteResp), nil
	}
	return resp, nil
}

/*

} else {
	parse next_token;
	if readLocal {
		call local PV;
	} else {
		call remote PV;
	}
}
*/

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(
	ctx context.Context, in *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	if in.GetNextToken() == "" {
		// When |next_token| in request is empty, there are two cases:
		// 1. The call does not need pagination.
		// 2. The call needs pagination, but this is the first call/page.
		//
		// To decide which case it is, we call local Mixer, and examine the
		// |next_token| in the response.

		resp, err := V2NodeCore(ctx, s.store, s.cache, s.metadata, in)
		if err != nil {
			return nil, err
		}

		// Call remote Mixer and merge only for non-paginated cases,
		// which are signaled by empty resp.GetNextToken().
		if resp.GetNextToken() == "" && s.metadata.RemoteMixerURL != "" {
			remoteResp := &pbv2.NodeResponse{}
			err := fetchRemote(s.metadata, s.httpClient, "/v2/node", in, remoteResp)
			if err != nil {
				return nil, err
			}
			return merger.MergeNode(resp, remoteResp), nil
		}

		return resp, nil
	} else { // in.GetNextToken() != ""
		paginationInfo, err := pagination.Decode(in.GetNextToken())
		if err != nil {
			return nil, err
		}

		if paginationInfo.GetReadFromRemote() {
			if s.metadata.RemoteMixerURL != "" {
				// From the perspective of remote Mixer, |read_from_remote| should be false.
				// Therefore, we update the |next_token| in request message to reflect that.
				paginationInfo.ReadFromRemote = false
				updatedNextToken, err := util.EncodeProto(paginationInfo)
				if err != nil {
					return nil, err
				}
				in.NextToken = updatedNextToken

				remoteResp := &pbv2.NodeResponse{}
				if err := fetchRemote(
					s.metadata, s.httpClient, "/v2/node", in, remoteResp); err != nil {
					return nil, err
				}
				return remoteResp, nil
			}
		} else { // Read from local.
			return V2NodeCore(ctx, s.store, s.cache, s.metadata, in)
		}
	}
	return nil, nil
}

// V2Event implements API for mixer.V2Event.
func (s *Server) V2Event(
	ctx context.Context, in *pbv2.EventRequest,
) (*pbv2.EventResponse, error) {
	resp, err := V2EventCore(ctx, s.store, in)
	if err != nil {
		return nil, err
	}
	if s.metadata.RemoteMixerURL != "" {
		remoteResp := &pbv2.EventResponse{}
		err := fetchRemote(s.metadata, s.httpClient, "/v2/event", in, remoteResp)
		if err != nil {
			return nil, err
		}
		return merger.MergeEvent(resp, remoteResp), nil
	}
	return resp, nil
}

// V2Observation implements API for mixer.V2Observation.
func (s *Server) V2Observation(
	ctx context.Context, in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	resp, err := V2ObservationCore(ctx, s.store, in)
	if err != nil {
		return nil, err
	}
	if s.metadata.RemoteMixerURL != "" {
		remoteResp := &pbv2.ObservationResponse{}
		err := fetchRemote(s.metadata, s.httpClient, "/v2/observation", in, remoteResp)
		if err != nil {
			return nil, err
		}
		return merger.MergeObservation(resp, remoteResp), nil
	}
	return resp, nil
}
