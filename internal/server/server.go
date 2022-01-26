// Copyright 2019 Google LLC
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
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/bigquery"
	cbt "cloud.google.com/go/bigtable"
	pubsub "cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/parser/mcf"
	dcpubsub "github.com/datacommonsorg/mixer/internal/pubsub"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/memdb"
	"github.com/datacommonsorg/mixer/internal/translator/solver"
	"github.com/datacommonsorg/mixer/internal/translator/types"
)

// Server holds resources for a mixer server
type Server struct {
	store    *store.Store
	metadata *resource.Metadata
	cache    *resource.Cache
}

func (s *Server) updateBranchTable(ctx context.Context, branchTableName string) {
	branchTable, err := NewBtTable(
		ctx, s.metadata.BtProject, s.metadata.BranchBtInstance, branchTableName)
	if err != nil {
		log.Printf("Failed to udpate branch cache Bigtable client: %v", err)
		return
	}
	s.store.BtGroup.UpdateBranchTable(branchTable)
	s.metadata.BranchTable = branchTableName
}

// ReadBranchTableName reads branch cache folder from GCS.
func ReadBranchTableName(
	ctx context.Context, bucket, versionFile string) (string, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", err
	}
	rc, err := client.Bucket(bucket).Object(versionFile).NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	folder, err := ioutil.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(folder), nil
}

// NewMetadata initialize the metadata for translator.
func NewMetadata(
	bqDataset, storeProject, branchInstance, schemaPath string) (*resource.Metadata, error) {
	_, filename, _, _ := runtime.Caller(0)
	subTypeMap, err := solver.GetSubTypeMap(
		path.Join(path.Dir(filename), "../translator/table_types.json"))
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(schemaPath)
	if err != nil {
		return nil, err
	}
	mappings := []*types.Mapping{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".mcf") {
			mappingStr, err := ioutil.ReadFile(filepath.Join(schemaPath, f.Name()))
			if err != nil {
				return nil, err
			}
			mapping, err := mcf.ParseMapping(string(mappingStr), bqDataset)
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, mapping...)
		}
	}
	outArcInfo := map[string]map[string][]types.OutArcInfo{}
	inArcInfo := map[string][]types.InArcInfo{}
	return &resource.Metadata{
			Mappings:         mappings,
			OutArcInfo:       outArcInfo,
			InArcInfo:        inArcInfo,
			SubTypeMap:       subTypeMap,
			Bq:               bqDataset,
			BtProject:        storeProject,
			BranchBtInstance: branchInstance,
		},
		nil
}

// NewBtTable creates a new cbt.Table instance.
func NewBtTable(
	ctx context.Context, projectID, instanceID, tableID string) (
	*cbt.Table, error) {
	btClient, err := cbt.NewClient(ctx, projectID, instanceID)
	if err != nil {
		return nil, err
	}
	return btClient.Open(tableID), nil
}

// SubscribeBranchCacheUpdate subscribe for branch cache update.
func (s *Server) SubscribeBranchCacheUpdate(
	ctx context.Context, pubsubProject, subscriberPrefix, pubsubTopic string,
) error {
	return dcpubsub.Subscribe(
		ctx,
		pubsubProject,
		subscriberPrefix,
		pubsubTopic,
		func(ctx context.Context, msg *pubsub.Message) error {
			branchTableName := string(msg.Data)
			log.Printf("Branch Cache Subscriber: use branch cache %s\n", branchTableName)
			s.updateBranchTable(ctx, branchTableName)
			return nil
		},
	)
}

// NewCache initializes the cache for stat var hierarchy.
func NewCache(ctx context.Context, baseTables []*cbt.Table) (*resource.Cache, error) {
	// TODO: [MERGE]: need to builc cache from multiple tables.
	rawSvg, err := statvar.GetRawSvg(ctx, baseTables[0])
	if err != nil {
		return nil, err
	}
	parentSvgMap := statvar.BuildParentSvgMap(rawSvg)
	searchIndex := statvar.BuildStatVarSearchIndex(rawSvg, false)
	blocklistedSearchIndex := statvar.BuildStatVarSearchIndex(rawSvg, true)

	return &resource.Cache{
		ParentSvg:                 parentSvgMap,
		SvgInfo:                   rawSvg,
		SvgSearchIndex:            searchIndex,
		BlocklistedSvgSearchIndex: blocklistedSearchIndex,
	}, nil
}

// NewMixerServer creates a new mixer server instance.
func NewMixerServer(
	bqClient *bigquery.Client,
	baseTables []*cbt.Table,
	branchTable *cbt.Table,
	metadata *resource.Metadata,
	cache *resource.Cache,
	memDb *memdb.MemDb,
) *Server {
	return &Server{
		store:    store.NewStore(bqClient, memDb, baseTables, branchTable),
		metadata: metadata,
		cache:    cache,
	}
}

// NewReconServer creates a new recon server instance.
func NewReconServer(
	baseTables []*cbt.Table,
) *Server {
	return &Server{
		store: store.NewStore(nil, nil, baseTables, nil),
	}
}
