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
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"

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
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/translator/solver"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/internal/util"
)

// Server holds resources for a mixer server
type Server struct {
	store                    *store.Store
	metadata                 *resource.Metadata
	cachedata                atomic.Pointer[cache.Cache]
	mapsClient               maps.MapsClient
	httpClient               *http.Client
	dispatcher               *dispatcher.Dispatcher
	flags                    *featureflags.Flags
	writeUsageLogs           bool
	embeddingsServerURL      string
	resolveEmbeddingsIndexes string
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

// NewMixerServer creates a new mixer server instance.
func NewMixerServer(
	store *store.Store,
	metadata *resource.Metadata,
	cachedata *cache.Cache,
	mapsClient maps.MapsClient,
	dispatcher *dispatcher.Dispatcher,
	flags *featureflags.Flags,
	writeUsageLogs bool,
	embeddingsServerURL string,
	resolveEmbeddingsIndexes string,
) *Server {
	s := &Server{
		store:                    store,
		metadata:                 metadata,
		cachedata:                atomic.Pointer[cache.Cache]{},
		mapsClient:               mapsClient,
		httpClient:               &http.Client{},
		dispatcher:               dispatcher,
		flags:                    flags,
		writeUsageLogs:           writeUsageLogs,
		embeddingsServerURL:      embeddingsServerURL,
		resolveEmbeddingsIndexes: resolveEmbeddingsIndexes,
	}
	s.cachedata.Store(cachedata)
	return s
}

// shouldDivertV2 returns true if the request should be diverted to the dispatcher.
func (s *Server) shouldDivertV2(ctx context.Context) bool {
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
