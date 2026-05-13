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
	"fmt"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"google.golang.org/protobuf/proto"
)

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

// GetSources returns the list of data source IDs.
func (dispatcher *Dispatcher) GetSources() []string {
	return dispatcher.sources.GetSources()
}

// handle handles a request lifecycle - pre-processing, core handling and post-processing.
func (dispatcher *Dispatcher) handle(requestContext *RequestContext, handler func(context.Context, proto.Message) (proto.Message, error)) (proto.Message, error) {
	for _, processor := range dispatcher.processors {
		outcome, err := (*processor).PreProcess(requestContext)
		if err != nil {
			return nil, err
		}
		switch outcome {
		case Done:
			return requestContext.CurrentResponse, nil
		case Continue:
			continue
		default:
			return nil, fmt.Errorf("invalid PreProcess outcome %v", outcome)
		}
	}

	response, err := handler(requestContext.Context, requestContext.CurrentRequest)
	if err != nil {
		return nil, err
	}

	requestContext.CurrentResponse = response

	for i := len(dispatcher.processors) - 1; i >= 0; i-- {
		processor := dispatcher.processors[i]
		outcome, err := (*processor).PostProcess(requestContext)
		if err != nil {
			return nil, err
		}
		switch outcome {
		case Done:
			return requestContext.CurrentResponse, nil
		case Continue:
			continue
		default:
			return nil, fmt.Errorf("invalid PostProcess outcome %v", outcome)
		}
	}

	return requestContext.CurrentResponse, nil
}

func (dispatcher *Dispatcher) Node(ctx context.Context, in *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeNode)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Node(ctx, request.(*pbv2.NodeRequest), pageSize)
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

func (dispatcher *Dispatcher) Sparql(ctx context.Context, in *pb.SparqlRequest) (*pb.QueryResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeSparql)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Sparql(ctx, request.(*pb.SparqlRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pb.QueryResponse), nil
}

func (dispatcher *Dispatcher) Event(ctx context.Context, in *pbv2.EventRequest) (*pbv2.EventResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeEvent)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.Event(ctx, request.(*pbv2.EventRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv2.EventResponse), nil
}

func (dispatcher *Dispatcher) BulkVariableInfo(ctx context.Context, in *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeBulkVariableInfo)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.BulkVariableInfo(ctx, request.(*pbv1.BulkVariableInfoRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv1.BulkVariableInfoResponse), nil
}

func (dispatcher *Dispatcher) BulkVariableGroupInfo(ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error) {
	requestContext := newRequestContext(ctx, in, TypeBulkVariableGroupInfo)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.BulkVariableGroupInfo(ctx, request.(*pbv1.BulkVariableGroupInfoRequest))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pbv1.BulkVariableGroupInfoResponse), nil
}

func newRequestContext(ctx context.Context, request proto.Message, requestType RequestType) *RequestContext {
	return &RequestContext{
		Context:         ctx,
		Type:            requestType,
		OriginalRequest: proto.Clone(request),
		CurrentRequest:  request,
	}
}

// SdmxData handles SDMX Data requests.
func (dispatcher *Dispatcher) SdmxData(ctx context.Context, in *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	requestContext := newRequestContext(ctx, in, TypeSdmxData)

	response, err := dispatcher.handle(requestContext, func(ctx context.Context, request proto.Message) (proto.Message, error) {
		return dispatcher.sources.SdmxData(ctx, request.(*pb.SdmxDataQuery))
	})

	if err != nil {
		return nil, err
	}
	return response.(*pb.SdmxDataResult), nil
}
