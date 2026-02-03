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
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/datacommonsorg/mixer/internal/log"
	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/statvar/search"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// V2Resolve implements API for mixer.V2Resolve.
func (s *Server) V2Resolve(
	ctx context.Context, in *pbv2.ResolveRequest,
) (*pbv2.ResolveResponse, error) {
	inProp, outProp, typeOfValues, err := validateAndParseResolveInputs(in)
	if err != nil {
		return nil, err
	}
	v2StartTime := time.Now()

	callLocal, callRemote, err := resolveRouting(in.GetTarget(), s.metadata.RemoteMixerDomain)
	if err != nil {
		return nil, err
	}

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

// parseResolvePropertyExpression parses and validates a property expression string.
//
// The expression generally takes the form "<-inProp->outProp", optionally with filters
// like "<-inProp{typeOf:Type}->outProp".
//
// Returns:
// - input property (string)
// - output property (string)
// - typeOf filter values ([]string, from the input property filter)
// - error if validation fails
func parseResolvePropertyExpression(prop string) (string, string, []string, error) {
	// Parse property expression into Arcs.
	arcs, err := v2.ParseProperty(prop)
	if err != nil {
		return "", "", nil, fmt.Errorf("Error parsing 'property' expression: %v", err)
	}

	if len(arcs) != 2 {
		return "", "", nil, fmt.Errorf("Invalid 'property' expression: must define exactly two parts (incoming and outgoing arcs). Found %d parts", len(arcs))
	}

	inArc := arcs[0]
	outArc := arcs[1]
	if inArc.Out || !outArc.Out {
		return "", "", nil, fmt.Errorf("Invalid 'property' expression: must start with an incoming arc and end with an outgoing arc")
	}

	if inArc.SingleProp == "" {
		return "", "", nil, fmt.Errorf("Invalid 'property' expression: input property must be provided")
	}
	if outArc.SingleProp == "" {
		return "", "", nil, fmt.Errorf("Invalid 'property' expression: output property must be provided")
	}

	var typeOfValues []string
	// Validate filters
	if len(inArc.Filter) > 0 {
		if len(inArc.Filter) > 1 {
			return "", "", nil, fmt.Errorf("Invalid 'property' expression: only 'typeOf' filter is supported")
		}
		if filter, ok := inArc.Filter["typeOf"]; !ok {
			for k := range inArc.Filter {
				return "", "", nil, fmt.Errorf("Invalid 'property' expression: invalid filter key '%s'. Only 'typeOf' filter is supported", k)
			}
		} else {
			typeOfValues = filter
		}
	}

	return inArc.SingleProp, outArc.SingleProp, typeOfValues, nil
}

// validateAndParseResolveInputs validates and parses the inputs for the resolve request.
//
// Validation logic:
// - `target`: Must be one of "base_only", "custom_only", "base_and_custom". Defaults to "base_and_custom".
// - `resolver`: Must be one of "place", "indicator". Defaults to "place".
// - `property`:
//   - Must match the format "<-inProp->outProp" (optionally with filters).
//   - "inProp" and "outProp" validation depends on the resolver:
//     - "place": if "inProp" is "description" or "geoCoordinate", "outProp" must be "dcid".
//     - "indicator": "inProp" must be "description" and "outProp" must be "dcid".
func validateAndParseResolveInputs(in *pbv2.ResolveRequest) (string, string, []string, error) {
	var validationErrors []string

	// Parse and validate `target`
	switch in.GetTarget() {
	case ResolveTargetBaseOnly, ResolveTargetCustomOnly, ResolveTargetBaseAndCustom:
	case "":
		// Set default value; ignored if current call is to base dc
		in.Target = ResolveTargetBaseAndCustom
	default:
		validationErrors = append(validationErrors, fmt.Sprintf("Invalid 'target': valid values are '%s', '%s', '%s'",
			ResolveTargetCustomOnly, ResolveTargetBaseOnly, ResolveTargetBaseAndCustom))
	}

	// Parse and validate `resolver`
	switch in.GetResolver() {
	case ResolveResolverPlace, ResolveResolverIndicator:
	case "":
		// Set default value
		in.Resolver = ResolveResolverPlace
	default:
		validationErrors = append(validationErrors, fmt.Sprintf("Invalid 'resolver': valid values are '%s', '%s'",
			ResolveResolverIndicator, ResolveResolverPlace))
	}

	// Parse `property` expression into its in arc property, out arc property and typeOf filter values
	if in.GetProperty() == "" {
		in.Property = ResolveDefaultPropertyExpression
	}

	inProp, outProp, typeOfValues,err := parseResolvePropertyExpression(in.GetProperty())
	if err != nil {
		validationErrors = append(validationErrors, err.Error())
	}

	// Validate property expression based on resolver
	if err == nil {
		switch in.GetResolver() {
		case ResolveResolverPlace:
			// geoCoordinate and description only support dcid as outArc
			if (inProp == DescriptionProperty || inProp == GeoCoordinateProperty) && outProp != DcidProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: given input property '%s', output property can only be '%s'",
					inProp, DcidProperty))
			}
		case ResolveResolverIndicator:
			// Indicator resolution only supports description as inArc.
			if inProp != DescriptionProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: indicator resolution only supports '%s' as input property",
					DescriptionProperty))
			}
			// Indicator resolution only supports dcid as outArc.
			if outProp != DcidProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: indicator resolution only supports '%s' as output property",
					DcidProperty))
			}
		}
	}

	if len(validationErrors) > 0 {
		return "", "", nil, status.Errorf(codes.InvalidArgument, "Invalid inputs in request. %s", strings.Join(validationErrors, ". "))
	}

	return inProp, outProp, typeOfValues, nil
}

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(ctx context.Context, in *pbv2.NodeRequest) (
	*pbv2.NodeResponse, error,
) {
	if rand.Float64() < s.flags.V2DivertFraction {
		slog.Info("V2Node request diverted to dispatcher backend", "request", in)
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
	if rand.Float64() < s.flags.V2DivertFraction {
		slog.Info("V2Observation request diverted to dispatcher backend", "request", in)
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

// V2Sparql implements API for Mixer.V2Sparql.
func (s *Server) V2Sparql(
	ctx context.Context, in *pb.SparqlRequest,
) (*pb.QueryResponse, error) {
	legacyRequest := &pb.QueryRequest{
		Sparql: in.Query,
	}
	return translator.Query(ctx, legacyRequest, s.metadata, s.store)
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

// resolveRouting determines whether to route to local and/or remote instances
// based on the target parameter and the presence of a remote mixer domain.
// Returns (shouldCallLocal, shouldCallRemote, error).
//
// Assumes that `target` has been validated.
//
// logic:
//   - If remoteMixerDomain is empty, we are the base instance (or standalone).
//     Always process locally, ignore target.
//   - If remoteMixerDomain is set, we are a custom instance.
//     Route based on target:
//   - "base_only": Call remote only.
//   - "custom_only": Call local only.
//   - "base_and_custom": Call both.
//   - Any other value defaults to calling both.
func resolveRouting(target, remoteMixerDomain string) (bool, bool, error) {
	if remoteMixerDomain == "" {
		return true, false, nil
	}
	switch target {
	case ResolveTargetBaseOnly:
		return false, true, nil
	case ResolveTargetCustomOnly:
		return true, false, nil
	default:
		return true, true, nil
	}
}
