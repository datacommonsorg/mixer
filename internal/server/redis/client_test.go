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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	v2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
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
	key, _ := generateCacheKey(request)
	anyMsg, _ := anypb.New(expectedResponse)
	marshaled, _ := proto.Marshal(anyMsg)
	cached, _ := util.Zip(marshaled)
	mock.ExpectSet(key, cached, 1*time.Minute).SetVal("OK")

	err := client.CacheResponse(ctx, request, expectedResponse)
	assert.NoError(t, err)

	// Test cache hit.
	mock.ExpectGet(key).SetVal(string(cached))

	found, err := client.GetCachedResponse(ctx, request, response)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, proto.Equal(expectedResponse, response))

	// Test cache miss.
	request2 := &v2.NodeRequest{Nodes: []string{"cacheMissNode"}}
	key2, _ := generateCacheKey(request2)
	mock.ExpectGet(key2).RedisNil()

	response2 := &v2.NodeResponse{}

	found, err = client.GetCachedResponse(ctx, request2, response2)
	assert.NoError(t, err)
	assert.False(t, found)
	assert.False(t, proto.Equal(expectedResponse, response2))

	// Test expiration.
	cacheClientExpired := newCacheClient(rdb, 1*time.Nanosecond)
	mock.ExpectSet(key, cached, 1*time.Nanosecond).SetVal("OK")
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

func TestCompressionSize(t *testing.T) {
	// Compressed sizes are larger for small messages and smaller for large messages.
	testCases := []struct {
		message          *pb.EntityInfo
		uncompressedSize int
		compressedSize   int
	}{
		{
			message: &pb.EntityInfo{
				Name:  "small",
				Types: repeat("type", 1),
			},
			uncompressedSize: 13,
			compressedSize:   42,
		},
		{
			message: &pb.EntityInfo{
				Name:  "medium",
				Types: repeat("type", 100),
			},
			uncompressedSize: 608,
			compressedSize:   49,
		},
		{
			message: &pb.EntityInfo{
				Name:  "large",
				Types: repeat("type", 100_000),
			},
			uncompressedSize: 600_007,
			compressedSize:   933,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message.Name, func(t *testing.T) {
			marshaled, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("Failed to marshal: %v", err)
			}

			compressed, err := util.Zip(marshaled)
			if err != nil {
				t.Fatalf("Failed to compress: %v", err)
			}

			uncompressedSize := len(marshaled)
			compressedSize := len(compressed)

			assert.Equal(t, tc.uncompressedSize, uncompressedSize)
			assert.Equal(t, tc.compressedSize, compressedSize)
		})
	}
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}
