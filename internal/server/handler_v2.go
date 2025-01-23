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

// Package server is the main server
package server

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// V2Resolve implements API for mixer.V2Resolve.
func (s *Server) V2Resolve(
	ctx context.Context, in *pbv2.ResolveRequest,
) (*pbv2.ResolveResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.ResolveResponse, 1)
	remoteRespChan := make(chan *pbv2.ResolveResponse, 1)

	errGroup.Go(func() error {
		localResp, err := s.V2ResolveCore(errCtx, in)
		if err != nil {
			return err
		}
		localRespChan <- localResp
		return nil
	})

	if s.metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.ResolveResponse{}
			err := util.FetchRemote(s.metadata, s.httpClient, "/v2/resolve", in, remoteResp)
			if err != nil {
				return err
			}
			remoteRespChan <- remoteResp
			return nil
		})
	} else {
		remoteRespChan <- nil
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localRespChan)
	close(remoteRespChan)
	localResp, remoteResp := <-localRespChan, <-remoteRespChan
	return merger.MergeResolve(localResp, remoteResp), nil
}

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(ctx context.Context, in *pbv2.NodeRequest) (
	*pbv2.NodeResponse, error,
) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.NodeResponse, 1)
	remoteRespChan := make(chan *pbv2.NodeResponse, 1)

	if in.GetNextToken() == "" {
		// When request |next_token| is empty, there are two cases:
		// 1. The call does not need pagination, e.g. PropertyLabels.
		// 2. The call needs pagination, but this is the first call/page.
		// In both cases, we need to read from both local and remote, and merge.

		errGroup.Go(func() error {
			resp, err := s.V2NodeCore(errCtx, in)
			if err != nil {
				return err
			}
			localRespChan <- resp
			return nil
		})

		if s.metadata.RemoteMixerDomain != "" {
			errGroup.Go(func() error {
				remoteResp := &pbv2.NodeResponse{}
				err := util.FetchRemote(s.metadata, s.httpClient, "/v2/node", in, remoteResp)
				if err != nil {
					return err
				}
				remoteRespChan <- remoteResp
				return nil
			})
		} else {
			remoteRespChan <- nil
		}
	} else {
		// in.GetNextToken() != ""
		// In this case, the call needs pagination, and it's not the first call/page.

		paginationInfo, err := pagination.Decode(in.GetNextToken())
		if err != nil {
			return nil, err
		}
		cursorGroups := paginationInfo.GetCursorGroups()
		remotePaginationInfo := paginationInfo.GetRemotePaginationInfo()

		if len(cursorGroups) > 0 {
			// Non-empty |cursor_groups|, read from local, for non-first page.
			errGroup.Go(func() error {
				resp, err := s.V2NodeCore(ctx, in)
				if err != nil {
					return err
				}
				localRespChan <- resp
				return nil
			})
		} else {
			localRespChan <- nil
		}

		if s.metadata.RemoteMixerDomain != "" && remotePaginationInfo != nil {
			// Read from remote, for non-first page.

			errGroup.Go(func() error {
				// Update |next_token| before sending the request to remote.
				// Peel off one layer of |remote_pagination_info| hierarchy.
				remoteReqNextToken, err := util.EncodeProto(remotePaginationInfo)
				if err != nil {
					return err
				}
				in.NextToken = remoteReqNextToken

				// Call remote.
				remoteResp := &pbv2.NodeResponse{}
				if err := util.FetchRemote(
					s.metadata, s.httpClient, "/v2/node", in, remoteResp); err != nil {
					return err
				}
				remoteRespChan <- remoteResp
				return nil
			})
		} else {
			remoteRespChan <- nil
		}
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localRespChan)
	close(remoteRespChan)

	localResp, remoteResp := <-localRespChan, <-remoteRespChan
	return merger.MergeNode(localResp, remoteResp)
}

// V2Event implements API for mixer.V2Event.
func (s *Server) V2Event(
	ctx context.Context, in *pbv2.EventRequest,
) (*pbv2.EventResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.EventResponse, 1)
	remoteRespChan := make(chan *pbv2.EventResponse, 1)

	errGroup.Go(func() error {
		localResp, err := s.V2EventCore(errCtx, in)
		if err != nil {
			return err
		}
		localRespChan <- localResp
		return nil
	})

	if s.metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.EventResponse{}
			err := util.FetchRemote(s.metadata, s.httpClient, "/v2/event", in, remoteResp)
			if err != nil {
				return err
			}
			remoteRespChan <- remoteResp
			return nil
		})
	} else {
		remoteRespChan <- nil
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localRespChan)
	close(remoteRespChan)
	localResp, remoteResp := <-localRespChan, <-remoteRespChan
	return merger.MergeEvent(localResp, remoteResp), nil
}

// V2Observation implements API for mixer.V2Observation.
func (s *Server) V2Observation(
	ctx context.Context, in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	initialResp, err := v2observation.ObservationInternal(
		ctx,
		s.store,
		s.cachedata.Load(),
		s.metadata,
		s.httpClient,
		in)
	if err != nil {
		return nil, err
	}
	calculatedResps, err := v2observation.MaybeCalculateHoles(
		ctx,
		s.store,
		s.cachedata.Load(),
		s.metadata,
		s.httpClient,
		in,
		initialResp,
	)
	if err != nil {
		return nil, err
	}
	// initialResp is preferred over any calculated response.
	combinedResp := append([]*pbv2.ObservationResponse{initialResp}, calculatedResps...)
	return merger.MergeMultiObservation(combinedResp), nil
}

// V2Sparql implements API for Mixer.V2Sparql.
func (s *Server) V2Sparql(
	ctx context.Context, in *pb.SparqlRequest,
) (*pb.QueryResponse, error) {
	legacyRequest := &pb.QueryRequest{
		Sparql: in.Query,
	}
	return translator.Query(ctx, legacyRequest, s.metadata, s.store)
}
