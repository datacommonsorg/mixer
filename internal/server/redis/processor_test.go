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
	"errors"
	"testing"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/go-redis/redismock/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
)

type MockCacheClient struct {
	mockResponse            *pbv2.NodeResponse
	GetCachedResponseCalled bool
}

func (m *MockCacheClient) GetCachedResponse(ctx context.Context, request proto.Message, response proto.Message) (bool, error) {
	proto.Merge(response, m.mockResponse)
	m.GetCachedResponseCalled = true
	return true, nil
}

func (m *MockCacheClient) CacheResponse(ctx context.Context, request proto.Message, response proto.Message) error {
	return nil
}
func TestCacheProcessorPreProcess(t *testing.T) {
	ctx := context.Background()

	// Test cases
	tests := []struct {
		name            string
		requestType     dispatcher.RequestType
		mockSetup       func(mock redismock.ClientMock)
		originalRequest proto.Message
		wantOutcome     dispatcher.Outcome
		wantErr         bool
		wantResponse    proto.Message
	}{
		{
			name:        "Cache Hit",
			requestType: dispatcher.TypeNode,
			mockSetup: func(mock redismock.ClientMock) {
				request := &pbv2.NodeRequest{Nodes: []string{"testNode"}}
				response := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}}
				key, _ := generateCacheKey(request)
				anyMsg, _ := anypb.New(response)
				marshaled, _ := proto.Marshal(anyMsg)
				cached, _ := util.Zip(marshaled)
				mock.ExpectGet(key).SetVal(string(cached))
			},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			wantOutcome:     dispatcher.Done,
			wantErr:         false,
			wantResponse:    &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}},
		},
		{
			name:        "Cache Miss",
			requestType: dispatcher.TypeNode,
			mockSetup: func(mock redismock.ClientMock) {
				request := &pbv2.NodeRequest{Nodes: []string{"testNode"}}
				key, _ := generateCacheKey(request)
				mock.ExpectGet(key).RedisNil()
			},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			wantOutcome:     dispatcher.Continue,
			wantErr:         false,
			wantResponse:    nil,
		},
		{
			name:        "Cache Error",
			requestType: dispatcher.TypeNode,
			mockSetup: func(mock redismock.ClientMock) {
				request := &pbv2.NodeRequest{Nodes: []string{"testNode"}}
				key, _ := generateCacheKey(request)
				mock.ExpectGet(key).SetErr(errors.New("redis error"))
			},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			wantOutcome:     dispatcher.Continue,
			wantErr:         false,
			wantResponse:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a mock Redis client
			rdb, mock := redismock.NewClientMock()

			// Call test.mockSetup with the mock
			test.mockSetup(mock)

			processor := NewCacheProcessor(newCacheClient(rdb, time.Minute))
			rc := &dispatcher.RequestContext{
				Context:         ctx,
				Type:            test.requestType,
				OriginalRequest: test.originalRequest,
			}

			outcome, err := processor.PreProcess(rc)

			// Check error
			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.wantOutcome, outcome)
			assert.True(t, proto.Equal(test.wantResponse, rc.CurrentResponse))

			// Ensure all expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestCacheProcessorPostProcess(t *testing.T) {
	ctx := context.Background()

	// Test cases
	tests := []struct {
		name            string
		requestType     dispatcher.RequestType
		mockSetup       func(mock redismock.ClientMock)
		originalRequest proto.Message
		currentResponse proto.Message
		wantOutcome     dispatcher.Outcome
		wantErr         bool
	}{
		{
			name:        "Cache Success",
			requestType: dispatcher.TypeNode,
			mockSetup: func(mock redismock.ClientMock) {
				request := &pbv2.NodeRequest{Nodes: []string{"testNode"}}
				response := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}}
				key, _ := generateCacheKey(request)
				anyMsg, _ := anypb.New(response)
				marshaled, _ := proto.Marshal(anyMsg)
				cached, _ := util.Zip(marshaled)
				mock.ExpectSet(key, cached, time.Minute).SetVal("OK")
			},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			currentResponse: &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}},
			wantOutcome:     dispatcher.Continue,
			wantErr:         false,
		},
		{
			name:        "Cache Error",
			requestType: dispatcher.TypeNode,
			mockSetup: func(mock redismock.ClientMock) {
				request := &pbv2.NodeRequest{Nodes: []string{"testNode"}}
				response := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}}
				key, _ := generateCacheKey(request)
				anyMsg, _ := anypb.New(response)
				marshaled, _ := proto.Marshal(anyMsg)
				cached, _ := util.Zip(marshaled)
				mock.ExpectSet(key, cached, time.Minute).SetErr(errors.New("redis error"))
			},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			currentResponse: &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}},
			wantOutcome:     dispatcher.Continue,
			wantErr:         false,
		},
		{
			name:            "No Response",
			requestType:     dispatcher.TypeNode,
			mockSetup:       func(mock redismock.ClientMock) {},
			originalRequest: &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			currentResponse: nil,
			wantOutcome:     dispatcher.Continue,
			wantErr:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a mock Redis client
			rdb, mock := redismock.NewClientMock()

			// Call test.mockSetup with the mock
			test.mockSetup(mock)

			processor := NewCacheProcessor(newCacheClient(rdb, time.Minute))
			rc := &dispatcher.RequestContext{
				Context:         ctx,
				Type:            test.requestType,
				OriginalRequest: test.originalRequest,
				CurrentResponse: test.currentResponse,
			}

			outcome, err := processor.PostProcess(rc)

			// Check error
			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.wantOutcome, outcome)

			// Ensure all expectations were met
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestNewEmptyResponse(t *testing.T) {
	// Test cases
	tests := []struct {
		name        string
		requestType dispatcher.RequestType
		want        proto.Message
	}{
		{
			name:        "Node",
			requestType: dispatcher.TypeNode,
			want:        &pbv2.NodeResponse{},
		},
		{
			name:        "Observation",
			requestType: dispatcher.TypeObservation,
			want:        &pbv2.ObservationResponse{},
		},
		{
			name:        "NodeSearch",
			requestType: dispatcher.TypeNodeSearch,
			want:        &pbv2.NodeSearchResponse{},
		},
		{
			name:        "Resolve",
			requestType: dispatcher.TypeResolve,
			want:        &pbv2.ResolveResponse{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := newEmptyResponse(test.requestType)
			assert.True(t, proto.Equal(test.want, got))
		})
	}
}

func TestSkipCache(t *testing.T) {
	skipCacheCtx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(
			util.XSkipCache, "true",
		),
	)

	// Test cases
	tests := []struct {
		name                    string
		ctx                     context.Context
		requestType             dispatcher.RequestType
		originalRequest         proto.Message
		getCachedResponseCalled bool
		currentResponse         proto.Message
		wantOutcome             dispatcher.Outcome
	}{
		{
			name:                    "Don't Skip Cache",
			ctx:                     context.Background(),
			requestType:             dispatcher.TypeNode,
			originalRequest:         &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			getCachedResponseCalled: true,
			currentResponse:         &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}},
			wantOutcome:             dispatcher.Done,
		},
		{
			name:                    "Skip Cache",
			ctx:                     skipCacheCtx,
			requestType:             dispatcher.TypeNode,
			originalRequest:         &pbv2.NodeRequest{Nodes: []string{"testNode"}},
			getCachedResponseCalled: false,
			currentResponse:         nil,
			wantOutcome:             dispatcher.Continue,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := &MockCacheClient{
				mockResponse: &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"testNode": {}}},
			}
			processor := NewCacheProcessor(m)
			rc := &dispatcher.RequestContext{
				Context:         test.ctx,
				Type:            test.requestType,
				OriginalRequest: test.originalRequest,
				CurrentResponse: test.currentResponse,
			}

			outcome, err := processor.PreProcess(rc)

			if err != nil {
				t.Errorf("Error runing TestSkipCache: %s", err)
			}

			assert.Equal(t, test.getCachedResponseCalled, m.GetCachedResponseCalled)
			if diff := cmp.Diff(rc.CurrentResponse, test.currentResponse, protocmp.Transform()); diff != "" {
				t.Errorf("TestSkipCache %s got diff: %s", test.name, diff)
			}
			assert.Equal(t, test.wantOutcome, outcome)
		})
	}
}
