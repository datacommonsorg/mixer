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

package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// defaultExpiration is the default expiration time for cached responses.
	defaultExpiration = 24 * time.Hour
	// cacheKeyPrefix is the prefix for all cache keys.
	cacheKeyPrefix = "mixer:"
	// protoRequestKeyPrefix is the prefix for all protobuf request cache keys.
	protoRequestKeyPrefix = cacheKeyPrefix + "request:"
)

// Interface to allow mocking.
type CacheClientInterface interface {
	GetCachedResponse(ctx context.Context, request proto.Message, response proto.Message) (bool, error)
	CacheResponse(ctx context.Context, request proto.Message, response proto.Message) error
}

// CacheClient handles Redis caching for protobuf messages.
type CacheClient struct {
	redisClient *redis.Client
	expiration  time.Duration
}

// NewCacheClient creates a new CacheClient from a yaml config string.
func NewCacheClient(redisConfigYaml string) (*CacheClient, error) {
	redisAddress, err := GetRedisAddress(redisConfigYaml)
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis address: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress,
		// Use default DB.
		DB: 0,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Connected to Redis at: %s", redisAddress)

	return newCacheClient(redisClient, defaultExpiration), nil
}
func newCacheClient(redisClient *redis.Client, expiration time.Duration) *CacheClient {
	return &CacheClient{
		redisClient: redisClient,
		expiration:  expiration,
	}
}

// Close closes the underlying redis connection.
func (c *CacheClient) Close() error {
	return c.redisClient.Close()
}

// generateCacheKey generates a unique cache key from a protobuf request.
func generateCacheKey(request proto.Message) (string, error) {
	marshaled, err := proto.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	hash := sha256.Sum256(marshaled)
	return protoRequestKeyPrefix + hex.EncodeToString(hash[:]), nil
}

// GetCachedResponse retrieves a cached protobuf response from Redis.
func (c *CacheClient) GetCachedResponse(ctx context.Context, request proto.Message, response proto.Message) (bool, error) {
	key, err := generateCacheKey(request)
	if err != nil {
		return false, err
	}

	cached, err := c.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// Cache miss.
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get from Redis: %w", err)
	}

	marshaled, err := util.Unzip([]byte(cached))
	if err != nil {
		return false, fmt.Errorf("failed to unzip: %w", err)
	}

	anyMsg := &anypb.Any{}
	if err := proto.Unmarshal(marshaled, anyMsg); err != nil {
		return false, fmt.Errorf("failed to unmarshal Any: %w", err)
	}

	if err := anyMsg.UnmarshalTo(response); err != nil {
		return false, fmt.Errorf("failed to unmarshal to response: %w", err)
	}

	return true, nil
}

// CacheResponse stores a protobuf response in Redis.
func (c *CacheClient) CacheResponse(ctx context.Context, request proto.Message, response proto.Message) error {
	key, err := generateCacheKey(request)
	if err != nil {
		return err
	}

	anyMsg, err := anypb.New(response)
	if err != nil {
		return fmt.Errorf("failed to create Any: %w", err)
	}

	marshaled, err := proto.Marshal(anyMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal Any: %w", err)
	}

	cached, err := util.Zip(marshaled)
	if err != nil {
		return fmt.Errorf("failed to zip: %w", err)
	}

	err = c.redisClient.Set(ctx, key, cached, c.expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set in Redis: %w", err)
	}

	return nil
}
