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
	"log"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"google.golang.org/protobuf/proto"
)

// CacheProcessor implements the dispatcher.Processor interface for performing caching operations.
type CacheProcessor struct {
	client *CacheClient
}

func NewCacheProcessor(client *CacheClient) *CacheProcessor {
	return &CacheProcessor{client: client}
}

func (processor *CacheProcessor) PreProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	cachedResponse := newEmptyResponse(rc.Type)
	if found, err := processor.client.GetCachedResponse(rc.Context, rc.OriginalRequest, cachedResponse); found {
		rc.CurrentResponse = cachedResponse
		return dispatcher.Done, err
	} else if err != nil {
		// Log the error but continue processing.
		log.Printf("Error getting cached response: %v", err)
	}
	return dispatcher.Continue, nil
}

func (processor *CacheProcessor) PostProcess(rc *dispatcher.RequestContext) (dispatcher.Outcome, error) {
	if rc.CurrentResponse != nil {
		if err := processor.client.CacheResponse(rc.Context, rc.OriginalRequest, rc.CurrentResponse); err != nil {
			// Log the error but continue processing.
			log.Printf("Error caching response: %v", err)
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
