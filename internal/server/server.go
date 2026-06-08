// Copyright 2023 Google LLC
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

package server

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	cbt "cloud.google.com/go/bigtable"
	pubsub "cloud.google.com/go/pubsub/v2"
	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/maps"
	"github.com/datacommonsorg/mixer/internal/metrics"
	"github.com/datacommonsorg/mixer/internal/parser/mcf"
	dcpubsub "github.com/datacommonsorg/mixer/internal/pubsub"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/topic"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/translator/solver"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/internal/agent"
)

// Server holds resources for a mixer server
type Server struct {
	store             *store.Store
	metadata          *resource.Metadata
	cachedata         atomic.Pointer[cache.Cache]
	mapsClient        maps.MapsClient
	httpClient        *http.Client
	dispatcher        *dispatcher.Dispatcher
	flags             *featureflags.Flags
	writeUsageLogs          bool
	embeddingsServiceClient *resolve.EmbeddingsServiceClient
	// Whether to use dispatcher flow with Spanner as a default datasource.
	useSpannerGraph   bool
	topicCacheManager *topic.TopicCacheManager
	agentService      *agent.Service

	// Centralized lifecycle scheduler registries
	initHooks         map[string]func(ctx context.Context) error
	periodicHooks     map[string]func(ctx context.Context) error

	// Background scheduling fields
	periodicTicker    *time.Ticker
	stopPeriodicCh    chan struct{}
	periodicWg        sync.WaitGroup
	periodicOnce      sync.Once
}

func (s *Server) updateBranchTable(ctx context.Context, branchTableName string) error {
	if s.store.BtGroup == nil {
		return nil
	}
	btClient, err := cbt.NewClient(ctx, bigtable.BranchBigtableProject, bigtable.BranchBigtableInstance)
	if err != nil {
		return err
	}
	branchTable := bigtable.NewBtTable(
		btClient,
		branchTableName,
	)
	s.store.BtGroup.UpdateBranchTable(
		bigtable.NewTable(branchTableName, branchTable, false /*isCustom=*/))
	slog.Info("Updated branch table", "table", branchTableName)
	return nil
}

// NewMetadata initialize the metadata for translator.
func NewMetadata(
	ctx context.Context,
	hostProject,
	bigQueryDataset,
	schemaPath,
	remoteMixerDomain string,
	foldRemoteRootSvg bool,
) (*resource.Metadata, error) {
	_, filename, _, _ := runtime.Caller(0)
	subTypeMap, err := solver.GetSubTypeMap(
		path.Join(path.Dir(filename), "../translator/table_types.json"))
	if err != nil {
		return nil, err
	}
	mappings := []*types.Mapping{}
	if schemaPath != "" && bigQueryDataset != "" {
		files, err := os.ReadDir(schemaPath)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".mcf") {
				mappingStr, err := os.ReadFile(filepath.Join(schemaPath, f.Name()))
				if err != nil {
					return nil, err
				}
				mapping, err := mcf.ParseMapping(string(mappingStr), bigQueryDataset)
				if err != nil {
					return nil, err
				}
				mappings = append(mappings, mapping...)
			}
		}
	}
	outArcInfo := map[string]map[string][]*types.OutArcInfo{}
	inArcInfo := map[string][]*types.InArcInfo{}

	var apiKey string
	if remoteMixerDomain != "" {
		apiKey, err = util.ReadLatestSecret(ctx, hostProject, util.MixerAPIKeyID)
		if err != nil {
			return nil, err
		}
	}

	return &resource.Metadata{
			Mappings:          mappings,
			OutArcInfo:        outArcInfo,
			InArcInfo:         inArcInfo,
			SubTypeMap:        subTypeMap,
			HostProject:       hostProject,
			BigQueryDataset:   bigQueryDataset,
			RemoteMixerDomain: remoteMixerDomain,
			RemoteMixerAPIKey: apiKey,
			FoldRemoteRootSvg: foldRemoteRootSvg,
		},
		nil
}

// SubscribeBranchCacheUpdate subscribe for branch cache update.
func (s *Server) SubscribeBranchCacheUpdate(ctx context.Context) error {
	return dcpubsub.Subscribe(
		ctx,
		bigtable.BranchBigtableProject,
		bigtable.BranchCacheSubscriberPrefix,
		bigtable.BranchCachePubsubTopic,
		func(ctx context.Context, msg *pubsub.Message) error {
			branchTableName := string(msg.Data)
			slog.Info("Branch cache subscriber message received", "table", branchTableName)
			return s.updateBranchTable(ctx, branchTableName)
		},
	)
}

// initAgentService initializes the decoupled agent.Service using Go's implicit satisfaction wiring.
func (s *Server) initAgentService() {
	s.agentService = agent.NewService(s, agent.NewCache(s))
}

// RegisterLifecycle registers the startup and periodic callbacks for a component under a single name key.
// Either callback can be nil if the component only requires one of the lifecycle phases.
func (s *Server) RegisterLifecycle(
	name string,
	initHook func(ctx context.Context) error,
	periodicHook func(ctx context.Context) error,
) {
	if initHook != nil {
		if s.initHooks == nil {
			s.initHooks = make(map[string]func(ctx context.Context) error)
		}
		s.initHooks[name] = initHook
	}
	if periodicHook != nil {
		if s.periodicHooks == nil {
			s.periodicHooks = make(map[string]func(ctx context.Context) error)
		}
		s.periodicHooks[name] = periodicHook
	}
}

// RunInitHooks executes all registered initialization callbacks concurrently in parallel.
// If any hook fails, it returns the first encountered error and aborts.
func (s *Server) RunInitHooks(ctx context.Context) error {
	slog.Info("Executing all registered server startup initialization hooks in parallel")
	return runHooksInParallel(ctx, s.initHooks, true /* failOnError */)
}

// StartPeriodicRefresher starts the centralized background goroutine to trigger periodic reloads.
func (s *Server) StartPeriodicRefresher(ctx context.Context, interval time.Duration) {
	s.periodicOnce.Do(func() {
		if interval <= 0 {
			slog.Info("Centralized periodic scheduler disabled (interval <= 0)")
			return
		}

		slog.Info("Starting centralized periodic scheduler", "interval", interval)
		s.periodicTicker = time.NewTicker(interval)
		s.stopPeriodicCh = make(chan struct{})
		s.periodicWg.Add(1)

		go func() {
			defer s.periodicWg.Done()
			defer s.periodicTicker.Stop()

			for {
				select {
				case <-s.stopPeriodicCh:
					return
				case <-ctx.Done():
					return
				case <-s.periodicTicker.C:
					slog.Info("Centralized periodic scheduler ticker triggered")
					_ = runHooksInParallel(ctx, s.periodicHooks, false /* failOnError */)
				}
			}
		}()
	})
}

// runHooksInParallel executes a collection of named callbacks concurrently.
// - If failOnError is true: blocks until all hooks finish and returns the first error (startup).
// - If failOnError is false: runs hooks asynchronously in background goroutines, logging failures (periodic refresh).
func runHooksInParallel(ctx context.Context, hooks map[string]func(ctx context.Context) error, failOnError bool) error {
	if len(hooks) == 0 {
		return nil
	}

	var g errgroup.Group
	var wg sync.WaitGroup
	for name, hook := range hooks {
		hName := name
		hFunc := hook

		// Declare the core execution logic exactly once in a local closure
		run := func() error {
			slog.Info("Executing hook", "hook", hName)
			start := time.Now()
			err := hFunc(ctx)
			if err != nil {
				slog.Error("Hook execution failed", "hook", hName, "error", err)
				if failOnError {
					return fmt.Errorf("hook %q: %w", hName, err)
				}
			} else {
				slog.Info("Hook completed successfully", "hook", hName, "duration", time.Since(start))
			}
			return nil
		}

		// Polymorphically dispatch the execution closure
		if failOnError {
			g.Go(run)
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = run()
			}()
		}
	}

	if failOnError {
		return g.Wait()
	}
	wg.Wait()
	return nil
}

// ClosePeriodicRefresher safely stops the background periodic scheduler goroutine and waits for exit.
func (s *Server) ClosePeriodicRefresher() {
	if s.stopPeriodicCh != nil {
		close(s.stopPeriodicCh)
		s.periodicWg.Wait()
	}
}


// NewMixerServer creates a new mixer server instance.
func NewMixerServer(
	store *store.Store,
	metadata *resource.Metadata,
	cachedata *cache.Cache,
	mapsClient maps.MapsClient,
	dispatcher *dispatcher.Dispatcher,
	flags *featureflags.Flags,
	writeUsageLogs bool,
	embeddingsServiceClient *resolve.EmbeddingsServiceClient,
	useSpannerGraph bool,
	topicCacheManager *topic.TopicCacheManager,
) *Server {
	s := &Server{
		store:                   store,
		metadata:                metadata,
		cachedata:               atomic.Pointer[cache.Cache]{},
		mapsClient:              mapsClient,
		httpClient:              &http.Client{},
		dispatcher:              dispatcher,
		flags:                   flags,
		writeUsageLogs:          writeUsageLogs,
		embeddingsServiceClient: embeddingsServiceClient,
		useSpannerGraph:         useSpannerGraph,
		topicCacheManager:       topicCacheManager,
	}
	s.cachedata.Store(cachedata)
	s.initAgentService()

	return s
}

// AgentService returns the internal agent.Service instance.
func (s *Server) AgentService() *agent.Service {
	return s.agentService
}

// TopicCacheManager returns the internal topic.TopicCacheManager instance.
func (s *Server) TopicCacheManager() *topic.TopicCacheManager {
	return s.topicCacheManager
}

// isSpannerInitialized returns true if the Spanner backend has been initialized.
func (s *Server) isSpannerInitialized() bool {
	return s.useSpannerGraph || (s.flags != nil && s.flags.UseSpannerGraph)
}

// shouldDivertV2 returns true if the request should be diverted to the dispatcher.
func (s *Server) shouldDivertV2(ctx context.Context) bool {
	if !s.isSpannerInitialized() {
		return false
	}
	if s.useSpannerGraph {
		return true
	}

	fraction := s.flags.V2DivertFraction
	if fraction <= 0 {
		return false
	}

	divert := rand.Float64() < fraction

	if divert {
		metrics.RecordV2Diversion(ctx)
	}
	return divert
}

