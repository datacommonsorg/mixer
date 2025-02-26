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
	"testing"
	"time"

	v2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestCacheClient(t *testing.T) {
	ctx := context.Background()

	// Create a mock Redis client
	rdb, mock := redismock.NewClientMock()

	client := newCacheClient(rdb, 1*time.Minute)

	request := &v2.NodeRequest{Nodes: []string{"testNode"}}
	response := &v2.NodeResponse{}
	expectedResponse := &v2.NodeResponse{Data: map[string]*v2.LinkedGraph{"testNode": {}}}

	// Mock CacheResponse.
	key, _ := client.generateCacheKey(request)
	anyMsg, _ := anypb.New(expectedResponse)
	marshaled, _ := proto.Marshal(anyMsg)
	mock.ExpectSet(key, marshaled, 1*time.Minute).SetVal("OK")

	err := client.CacheResponse(ctx, request, expectedResponse)
	assert.NoError(t, err)

	// Test cache hit.
	mock.ExpectGet(key).SetVal(string(marshaled))

	found, err := client.GetCachedResponse(ctx, request, response)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, proto.Equal(expectedResponse, response))

	// Test cache miss.
	request2 := &v2.NodeRequest{Nodes: []string{"cacheMissNode"}}
	key2, _ := client.generateCacheKey(request2)
	mock.ExpectGet(key2).RedisNil()

	response2 := &v2.NodeResponse{}

	found, err = client.GetCachedResponse(ctx, request2, response2)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.False(t, proto.Equal(expectedResponse, response2))

	//Test expiration.
	cacheClientExpired := newCacheClient(rdb, 1*time.Nanosecond)
	mock.ExpectSet(key, marshaled, 1*time.Nanosecond).SetVal("OK")
	err = cacheClientExpired.CacheResponse(ctx, request, expectedResponse)
	assert.NoError(t, err)

	mock.ExpectGet(key).RedisNil()
	time.Sleep(10 * time.Millisecond)

	found, err = cacheClientExpired.GetCachedResponse(ctx, request, response)
	assert.NoError(t, err)
	assert.False(t, found)

	// Ensure all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet())
}
