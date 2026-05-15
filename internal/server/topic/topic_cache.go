// Copyright 2026 Google LLC
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

// Package topic manages the in-memory and Redis caching of Knowledge Graph topic hierarchies.
// This file (topic_cache.go) implements the core cache manager, concurrency lifecycle (Start/Close),
// and synchronized multi-tier accessors (L1 memory and L2 Redis).
package topic

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/redis"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	defaultPageSize      = 1000
	rootTopicThreshold   = 10
	defaultRootTopicDcid = "dc/topic/Root"
)

var (
	redisCacheKeyProto = &wrapperspb.StringValue{Value: "topic/topic_cache"}
)

// TopicVariableCache is an in-memory composite struct caching server-wide topic hierarchy
// and variable metadata.
type TopicVariableCache struct {
	TopicHierarchy *pb.TopicHierarchy
	// SVs map[string]*SVInfo // Placeholder for follow-up task
}

// String implements fmt.Stringer to provide a concise summary of the cached hierarchy contents.
func (c *TopicVariableCache) String() string {
	if c == nil || c.TopicHierarchy == nil {
		return "TopicVariableCache{topicCount: 0, rootCount: 0}"
	}
	return fmt.Sprintf("TopicVariableCache{topicCount: %d, rootCount: %d}", len(c.TopicHierarchy.GetTopics()), len(c.TopicHierarchy.GetRootTopicDcids()))
}

// TopicCacheManager manages the loading, building, and caching of topics.
type TopicCacheManager struct {
	ds          datasource.DataSource
	redisClient redis.CacheClient

	mu    sync.RWMutex
	cache *TopicVariableCache

	ticker    *time.Ticker
	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewTopicCacheManager creates a new TopicCacheManager.
func NewTopicCacheManager(ds datasource.DataSource, redisClient redis.CacheClient) *TopicCacheManager {
	return &TopicCacheManager{
		ds:          ds,
		redisClient: redisClient,
	}
}

// Start starts the background goroutine to periodically refresh the topic hierarchy cache from the KG.
// It performs an initial synchronous load before starting the background ticker loop.
func (m *TopicCacheManager) Start(ctx context.Context, interval time.Duration) {
	m.startOnce.Do(func() {
		slog.Info("Performing initial topic cache load")
		if _, err := m.LoadHierarchy(ctx); err != nil {
			slog.Error("Error during initial topic cache load", "error", err)
		}

		m.ticker = time.NewTicker(interval)
		m.stopCh = make(chan struct{})
		m.wg.Add(1)

		go func() {
			defer m.wg.Done()
			defer m.ticker.Stop()

			for {
				select {
				case <-m.stopCh:
					return
				case <-ctx.Done():
					return
				case <-m.ticker.C:
					slog.Info("Background topic cache refresher triggered")
					if _, err := m.LoadHierarchy(ctx); err != nil {
						slog.Error("Error refreshing topic hierarchy", "error", err)
					}
				}
			}
		}()
	})
}

// Close stops the background refresher goroutine.
func (m *TopicCacheManager) Close() {
	m.stopOnce.Do(func() {
		if m.stopCh != nil {
			close(m.stopCh)
		}
		m.wg.Wait()
	})
}

// CachedHierarchy returns the currently cached TopicHierarchy in local L1 memory, or nil if empty.
// Note: The returned TopicHierarchy pointer references the live in-memory cache and must be treated as read-only by callers.
func (m *TopicCacheManager) CachedHierarchy() *pb.TopicHierarchy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cache == nil {
		return nil
	}
	return m.cache.TopicHierarchy
}

// Update thread-safely updates the internal in-memory TopicVariableCache.
// Note: Updating the hierarchy replaces the entire composite cache object. Any other cached metadata
// (such as Statistical Variables) will be refreshed/repopulated alongside the hierarchy.
func (m *TopicCacheManager) Update(hierarchy *pb.TopicHierarchy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = &TopicVariableCache{
		TopicHierarchy: hierarchy,
	}

	slog.Info("Topic cache updated in memory", "cache", m.cache.String())
}

// GetHierarchy retrieves the cached TopicHierarchy.
// It first checks the local L1 in-memory cache.
// If L1 is empty/cold, it falls back to LoadHierarchy to load it from Redis or the KG.
func (m *TopicCacheManager) GetHierarchy(ctx context.Context) (*pb.TopicHierarchy, error) {
	if h := m.CachedHierarchy(); h != nil {
		return h, nil
	}
	return m.LoadHierarchy(ctx)
}

// LoadHierarchy loads the TopicHierarchy either from the L2 Redis cache or synchronously from the KG.
// It populates both L1 and L2 caches upon loading.
func (m *TopicCacheManager) LoadHierarchy(ctx context.Context) (*pb.TopicHierarchy, error) {
	defer util.TimeTrack(time.Now(), "topic: LoadHierarchy")
	// Try loading from L2 Redis cache
	if m.redisClient != nil {
		var cachedHierarchy pb.TopicHierarchy
		if found, err := m.redisClient.GetCachedResponse(ctx, redisCacheKeyProto, &cachedHierarchy); found && err == nil {
			slog.Info("Topic cache hit in Redis")
			m.Update(&cachedHierarchy)
			return &cachedHierarchy, nil
		} else if err != nil {
			slog.Error("Failed to read topic cache from Redis", "error", err)
		}
	}

	// L2 Miss: synchronous load from KG
	slog.Info("Topic cache miss: loading synchronously from KG")
	hierarchy, err := m.FetchTopicsFromKG(ctx)
	if err != nil {
		slog.Error("Failed to load topic cache from KG during miss", "error", err)
		return nil, fmt.Errorf("failed to load topic cache from KG during miss: %w", err)
	}

	m.Update(hierarchy)

	// Populate Redis warm L2 cache
	if m.redisClient != nil {
		if err := m.redisClient.CacheResponse(ctx, redisCacheKeyProto, hierarchy); err != nil {
			slog.Error("Failed to write topic cache to Redis", "error", err)
		} else {
			slog.Info("Saved topic cache in Redis successfully")
		}
	}

	return hierarchy, nil
}
