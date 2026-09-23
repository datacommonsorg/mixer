// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"context"
	"fmt"
	"log/slog"
	"unicode/utf8"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// maxCacheResponseBytes is the upper size limit (20 MB) for caching API
	// response payloads in Redis.
	maxCacheResponseBytes = 20 * 1024 * 1024 // 20 MB
	maxLoggedRequestBytes = 1024
	truncatedSuffix       = " ... [truncated]"
)

var logMarshalOptions = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: false,
}

// CacheProcessor implements the dispatcher.Processor interface for performing caching operations.
type CacheProcessor struct {
	client           CacheClient
	maxResponseBytes int
}

func NewCacheProcessor(client CacheClient) *CacheProcessor {
	return newCacheProcessorWithLimit(client, maxCacheResponseBytes)
}

func newCacheProcessorWithLimit(client CacheClient, maxResponseBytes int) *CacheProcessor {
	return &CacheProcessor{
		client:           client,
		maxResponseBytes: maxResponseBytes,
	}
}

func (processor *CacheProcessor) PreProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	if skipCache(rc.Context) {
		return dispatcher.Continue, nil
	}

	cachedResponse, err := newEmptyResponse(rc.Type)
	if err != nil {
		slog.Error("Skipping cache", "error", err)
		return dispatcher.Continue, err
	}
	if found, err := processor.client.GetCachedResponse(rc.Context, rc.OriginalRequest, cachedResponse); found {
		slog.Info("Cache hit", "originalRequest", rc.OriginalRequest)

		rc.CurrentResponse = cachedResponse
		return dispatcher.Done, err
	} else if err != nil {
		// Log the error but continue processing.
		slog.Error("Error getting cached response", "error", err)
	}
	return dispatcher.Continue, nil
}

// PostProcess stores the returned response in Redis if caching is enabled and
// the response size does not exceed maxResponseBytes. Responses exceeding the
// limit are logged and returned to the caller without caching.
func (processor *CacheProcessor) PostProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	if rc == nil || rc.OriginalRequest == nil || rc.CurrentResponse == nil {
		return dispatcher.Continue, nil
	}
	if skipCache(rc.Context) {
		return dispatcher.Continue, nil
	}
	if processor.maxResponseBytes > 0 {
		size := proto.Size(rc.CurrentResponse)
		if size > processor.maxResponseBytes {
			slog.Warn("Skipping Redis cache for large response payload",
				"requestType", rc.Type,
				"sizeBytes", size,
				"limitBytes", processor.maxResponseBytes,
				"request", formatRequestForLog(rc.OriginalRequest),
			)
			return dispatcher.Continue, nil
		}
	}
	if err := processor.client.CacheResponse(rc.Context, rc.OriginalRequest, rc.CurrentResponse); err != nil {
		// Log the error but continue processing.
		slog.Error("Error caching response", "error", err)
	}
	return dispatcher.Continue, nil
}

// formatRequestForLog serializes req to compact JSON for debug logging,
// truncating payloads larger than maxLoggedRequestBytes on a valid UTF-8 rune
// boundary without allocating a full-size string copy first.
func formatRequestForLog(req proto.Message) string {
	if req == nil {
		return ""
	}
	bytes, err := logMarshalOptions.Marshal(req)
	if err != nil {
		return ""
	}
	if len(bytes) <= maxLoggedRequestBytes {
		return string(bytes)
	}
	end := maxLoggedRequestBytes
	for end > 0 && !utf8.RuneStart(bytes[end]) {
		end--
	}
	return string(bytes[:end]) + truncatedSuffix
}

// newEmptyResponse returns a new empty response for the given request type.
func newEmptyResponse(requestType dispatcher.RequestType) (proto.Message, error) {
	switch requestType {
	case dispatcher.TypeNode:
		return &pbv2.NodeResponse{}, nil
	case dispatcher.TypeNodeSearch:
		return &pbv2.NodeSearchResponse{}, nil
	case dispatcher.TypeObservation:
		return &pbv2.ObservationResponse{}, nil
	case dispatcher.TypeResolve:
		return &pbv2.ResolveResponse{}, nil
	case dispatcher.TypeSparql:
		return &pb.QueryResponse{}, nil
	case dispatcher.TypeEvent:
		return &pbv2.EventResponse{}, nil
	case dispatcher.TypeBulkVariableInfo:
		return &pbv1.BulkVariableInfoResponse{}, nil
	case dispatcher.TypeBulkVariableGroupInfo:
		return &pbv1.BulkVariableGroupInfoResponse{}, nil
	case dispatcher.TypeSdmxData:
		return &sdmxpb.SdmxDataResult{}, nil
	case dispatcher.TypeSdmxAvailability:
		return &sdmxpb.SdmxAvailabilityResult{}, nil
	case dispatcher.TypeFilterStatVarsByEntity:
		return &pb.FilterStatVarsByEntityResponse{}, nil
	default:
		return nil, fmt.Errorf("unknown request type for caching: %v", requestType)
	}
}

// skipCache checks whether to skip Redis cache.
func skipCache(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XSkipCache)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}
