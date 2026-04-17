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
	"log/slog"
	"time"

	"github.com/datacommonsorg/mixer/internal/log"
	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/statvar/search"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// V2Resolve implements API for mixer.V2Resolve.
func (s *Server) V2Resolve(
	ctx context.Context, in *pbv2.ResolveRequest,
) (*pbv2.ResolveResponse, error) {
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.Resolve(ctx, in)
	}

	v2StartTime := time.Now()

	inProp, outProp, typeOfValues, err := resolve.ValidateAndParseResolveInputs(in)
	if err != nil {
		return nil, err
	}

	hasRemoteMixerDomain := s.metadata.RemoteMixerDomain != ""
	callLocal, callRemote := resolve.ResolveRouting(in.GetTarget(), hasRemoteMixerDomain)

	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.ResolveResponse, 1)
	remoteRespChan := make(chan *pbv2.ResolveResponse, 1)

	if callLocal {
		errGroup.Go(func() error {
			localResp, err := s.V2ResolveCore(errCtx, in, inProp, outProp, typeOfValues)
			if err != nil {
				return err
			}
			localRespChan <- localResp
			return nil
		})
	} else {
		localRespChan <- nil
	}

	if callRemote {
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

	// Note: merger.MergeResolve handles nil inputs (e.g. error handling or empty) gracefully
	v2Resp := merger.MergeResolve(localResp, remoteResp)
	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3Resolve(ctx, req.(*pbv2.ResolveRequest))
		},
		GetV2ResolveCmpOpts(),
	)

	return v2Resp, nil
}

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(ctx context.Context, in *pbv2.NodeRequest) (
	*pbv2.NodeResponse, error,
) {
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.Node(ctx, in, datasources.DefaultPageSize)
	}

	v2StartTime := time.Now()
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
	v2Resp, err := merger.MergeNode(localResp, remoteResp)
	if err != nil {
		return nil, err
	}
	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3Node(ctx, req.(*pbv2.NodeRequest))
		},
		GetV2NodeCmpOpts(),
	)

	return v2Resp, nil
}

// V2Event implements API for mixer.V2Event.
func (s *Server) V2Event(
	ctx context.Context, in *pbv2.EventRequest,
) (*pbv2.EventResponse, error) {
	// Divert to dispatcher if enabled.
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.Event(ctx, in)
	}

	// Handle V2 Event.
	v2StartTime := time.Now()
	v2Resp, err := s.handleV2Event(ctx, in)
	if err != nil {
		return nil, err
	}
	v2Latency := time.Since(v2StartTime)

	// Mirror to V3 for comparison.
	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3Event(ctx, req.(*pbv2.EventRequest))
		},
		GetV2EventCmpOpts(),
	)

	return v2Resp, nil
}

// handleV2Event wraps the core V2 implementation of mixer.V2Event.
func (s *Server) handleV2Event(
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
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.Observation(ctx, in)
	}

	v2StartTime := time.Now()

	surface, toRemote := util.GetMetadata(ctx)

	initialResp, queryType, err := v2observation.ObservationInternal(
		ctx,
		s.store,
		s.cachedata.Load(),
		s.metadata,
		s.httpClient,
		in,
		surface)
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
		surface,
	)
	if err != nil {
		return nil, err
	}
	// initialResp is preferred over any calculated response.
	combinedResp := append([]*pbv2.ObservationResponse{initialResp}, calculatedResps...)
	v2Resp := merger.MergeMultiObservation(combinedResp)
	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3Observation(ctx, req.(*pbv2.ObservationRequest))
		},
		GetV2ObservationCmpOpts(),
	)

	// Create a new ID to return as a header on the response.
	// This is used for usage logging and in the website to log cached usage.
	responseId := uuid.New()
	if err := grpc.SetHeader(ctx, metadata.Pairs("x-response-id", responseId.String())); err != nil {
		slog.Warn("Failed to set responseId header", "error", err)
	}

	// Handle usage logging.
	if s.writeUsageLogs {
		log.WriteUsageLog(surface, toRemote, v2Resp, queryType, responseId.String())
	}

	return v2Resp, nil
}

// V2RecognizePlaces implements API for Mixer.V2RecognizePlaces.
func (s *Server) V2RecognizePlaces(
	ctx context.Context, in *pb.RecognizePlacesRequest,
) (*pb.RecognizePlacesResponse, error) {
	return recon.RecognizePlaces(ctx, in, s.store.RecogPlaceStore, false)
}

// V2Sparql implements API for Mixer.V2Sparql.
func (s *Server) V2Sparql(
	ctx context.Context, in *pb.SparqlRequest,
) (*pb.QueryResponse, error) {
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.Sparql(ctx, in)
	}

	v2StartTime := time.Now()

	legacyRequest := &pb.QueryRequest{
		Sparql: in.Query,
	}
	v2Resp, err := translator.Query(ctx, legacyRequest, s.metadata, s.store)
	if err != nil {
		return nil, err
	}
	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3Sparql(ctx, req.(*pb.SparqlRequest))
		},
		GetV2SparqlCmpOpts(),
	)

	return v2Resp, nil
}

// FilterStatVarsByEntity implements API for Mixer.FilterStatVarsByEntity.
func (s *Server) FilterStatVarsByEntity(
	ctx context.Context, in *pb.FilterStatVarsByEntityRequest,
) (*pb.FilterStatVarsByEntityResponse, error) {
	errGroup, _ := errgroup.WithContext(ctx)

	localResponseChan := make(chan *pb.FilterStatVarsByEntityResponse, 1)
	remoteResponseChan := make(chan *pb.FilterStatVarsByEntityResponse, 1)

	errGroup.Go(func() error {
		localResponse, err := search.FilterStatVarsByEntity(ctx, in, s.store, s.cachedata.Load())
		if err != nil {
			return err
		}
		localResponseChan <- localResponse
		return nil
	})

	remoteResponse := &pb.FilterStatVarsByEntityResponse{}
	if s.metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			if err := util.FetchRemote(
				s.metadata,
				s.httpClient,
				"/v2/variable/filter",
				in,
				remoteResponse,
			); err != nil {
				return err
			}
			remoteResponseChan <- remoteResponse
			return nil
		})
	} else {
		remoteResponseChan <- nil
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localResponseChan)
	close(remoteResponseChan)

	merged := merger.MergeFilterStatVarsByEntityResponse(<-localResponseChan, <-remoteResponseChan)
	return merged, nil
}

// V2BulkVariableInfo implements API for Mixer.V2BulkVariableInfo.
func (s *Server) V2BulkVariableInfo(
	ctx context.Context, in *pbv1.BulkVariableInfoRequest,
) (*pbv1.BulkVariableInfoResponse, error) {
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.BulkVariableInfo(ctx, in)
	}

	v2StartTime := time.Now()

	// Use the V1 implementation for now.
	v2Resp, err := s.BulkVariableInfo(ctx, in)
	if err != nil {
		return nil, err
	}

	// The new response will not contain all legacy V1 fields.
	// To ensure the new response is sufficient, clear all legacy fields until swapping over to the new backend.
	for _, varInfo := range v2Resp.GetData() {
		if varInfo.GetInfo() == nil {
			continue
		}
		varInfo.Info.PlaceTypeSummary = map[string]*pb.StatVarSummary_PlaceTypeSummary{}
		for _, provSummary := range varInfo.GetInfo().GetProvenanceSummary() {
			provSummary.ReleaseFrequency = ""
			for _, seriesSummary := range provSummary.GetSeriesSummary() {
				for key := range seriesSummary.GetPlaceTypeSummary() {
					seriesSummary.PlaceTypeSummary[key] = &pb.StatVarSummary_PlaceTypeSummary{}
				}
				seriesSummary.MinValue = nil
				seriesSummary.MaxValue = nil
			}
		}
	}

	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3BulkVariableInfo(ctx, req.(*pbv1.BulkVariableInfoRequest))
		},
		GetV2BulkVariableInfoCmpOpts(),
	)

	return v2Resp, nil
}

// V2BulkVariableGroupInfo implements API for Mixer.V2BulkVariableGroupInfo.
func (s *Server) V2BulkVariableGroupInfo(
	ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest,
) (*pbv1.BulkVariableGroupInfoResponse, error) {
	if s.shouldDivertV2(ctx) {
		return s.dispatcher.BulkVariableGroupInfo(ctx, in)
	}

	v2StartTime := time.Now()

	// Use the V1 implementation for now.
	v1Resp, err := s.BulkVariableGroupInfo(ctx, in)
	if err != nil {
		return nil, err
	}
	v2Resp := proto.Clone(v1Resp).(*pbv1.BulkVariableGroupInfoResponse)

	convertV1ToV2BulkVariableGroupInfo(v2Resp)

	v2Latency := time.Since(v2StartTime)

	s.maybeMirrorV3(
		ctx,
		in,
		v2Resp,
		v2Latency,
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return s.V3BulkVariableGroupInfo(ctx, req.(*pbv1.BulkVariableGroupInfoRequest))
		},
		GetV2BulkVariableGroupInfoCmpOpts(),
	)

	return v2Resp, nil
}

// convertV1ToV2BulkVariableGroupInfo converts a V1 BulkVariableGroupInfoResponse to the V2 version.
func convertV1ToV2BulkVariableGroupInfo(resp *pbv1.BulkVariableGroupInfoResponse) {
	// The new response will not contain all legacy V1 fields.
	// To ensure the new response is sufficient, clear all legacy fields until swapping over to the new backend.
	for _, info := range resp.GetData() {
		if info.GetInfo() == nil {
			continue
		}
		info.Info.ParentStatVarGroups = nil
		for _, childSV := range info.GetInfo().GetChildStatVars() {
			childSV.SearchName = ""
			childSV.SearchNames = nil
			childSV.Definition = ""
		}
	}
}
