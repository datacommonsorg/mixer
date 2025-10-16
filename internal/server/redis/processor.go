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
	"log/slog"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// CacheProcessor implements the dispatcher.Processor interface for performing caching operations.
type CacheProcessor struct {
	client CacheClient
}

func NewCacheProcessor(client CacheClient) *CacheProcessor {
	return &CacheProcessor{client: client}
}

func (processor *CacheProcessor) PreProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	if skipCache(rc.Context) {
		return dispatcher.Continue, nil
	}

	cachedResponse := newEmptyResponse(rc.Type)
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

// Stores the returned response in Redis if caching is enabled for the request.
func (processor *CacheProcessor) PostProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	if skipCache(rc.Context) {
		return dispatcher.Continue, nil
	}
	if rc.CurrentResponse != nil {
		if err := processor.client.CacheResponse(rc.Context, rc.OriginalRequest, rc.CurrentResponse); err != nil {
			// Log the error but continue processing.
			slog.Error("Error caching response", "error", err)
		}
	}
	return dispatcher.Continue, nil
}

// newEmptyResponse returns a new empty response for the given request type.
func newEmptyResponse(requestType dispatcher.RequestType) proto.Message {
	switch requestType {
	case dispatcher.TypeNode:
		return &pbv2.NodeResponse{}
	case dispatcher.TypeNodeSearch:
		return &pbv2.NodeSearchResponse{}
	case dispatcher.TypeObservation:
		return &pbv2.ObservationResponse{}
	case dispatcher.TypeResolve:
		return &pbv2.ResolveResponse{}
	default:
		return nil
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
