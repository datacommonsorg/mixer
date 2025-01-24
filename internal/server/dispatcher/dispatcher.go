// Copyright 2025 Google LLC
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

package dispatcher

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"google.golang.org/protobuf/proto"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// RequestType represents the type of request.
type RequestType string

const (
	TypeNode        RequestType = "Node"
	TypeNodeSearch  RequestType = "NodeSearch"
	TypeObservation RequestType = "Observation"
	TypeResolve     RequestType = "Resolve"
)

// RequestContext holds the context for a given request.

// NOTE: We are using the base proto.Message for requests and responses.
// Other options were using generics or going with different *RequestContext struct for each type of request.
// The downside of using a base type is that it needs casting wherever it it used.
// The upside is that we only have one context object
// and if there aren't many processors that deal with the RequestContext, casting in a small number of places is ok.
// We can revisit and use a different approach if this proves to be cumbersome.
type RequestContext struct {
	context.Context
	Type            RequestType
	OriginalRequest proto.Message
	CurrentRequest  proto.Message
	CurrentResponse proto.Message
}

// Processor interface defines methods for performing pre and post processing operations.
type Processor interface {
	PreProcess(*RequestContext) error
	PostProcess(*RequestContext) error
}

// Dispatcher struct handles requests by dispatching requests to various processors and datasources as appropriate.
type Dispatcher struct {
	processors []*Processor
	sources    *datasources.DataSources
}

func NewDispatcher(processors []*Processor, sources *datasources.DataSources) *Dispatcher {
	return &Dispatcher{
		processors: processors,
		sources:    sources,
	}
}

// handle handles a request lifecycle - pre-processing, core handling and post-processing.
func (dispatcher *Dispatcher) handle(requestContext *RequestContext, handler func(context.Context, proto.Message) (proto.Message, error)) (proto.Message, error) {
	for _, processor := range dispatcher.processors {
		if err := (*processor).PreProcess(requestContext); err != nil {
			return nil, err
		}
	}

	response, err := handler(requestContext.Context, requestContext.CurrentRequest)
	if err != nil {
		return nil, err
	}

	requestContext.CurrentResponse = response

	for _, processor := range dispatcher.processors {
		if err := (*processor).PostProcess(requestContext); err != nil {
			return nil, err
		}
	}

	return requestContext.CurrentResponse, nil
}

func (dispatcher *Dispatcher) Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeNode)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Node(ctx, request.(*pbv2.NodeRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv2.NodeResponse), nil
}

func (dispatcher *Dispatcher) Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeObservation)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Observation(ctx, request.(*pbv2.ObservationRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv2.ObservationResponse), nil
}

func (dispatcher *Dispatcher) NodeSearch(ctx context.Context, in *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeNodeSearch)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.NodeSearch(ctx, request.(*pbv2.NodeSearchRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv2.NodeSearchResponse), nil
}

func (dispatcher *Dispatcher) Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeResolve)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Resolve(ctx, request.(*pbv2.ResolveRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv2.ResolveResponse), nil
}

func newRequestContext(ctx context.Context, request proto.Message, requestType RequestType) *RequestContext {
	return &RequestContext{
		Context:         ctx,
		Type:            requestType,
		OriginalRequest: proto.Clone(request),
		CurrentRequest:  request,
	}
}
