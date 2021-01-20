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
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/base"
	"github.com/datacommonsorg/mixer/internal/translator"
	"github.com/datacommonsorg/mixer/internal/util"
)

// Server holds resources for a mixer server
type Server struct {
	bqClient       *bigquery.Client
	branchBtClient *bigtable.Client
	btTables       []*bigtable.Table
	metadata       *Metadata
}

func (s *Server) updateBranchTable(ctx context.Context, branchTableName string) {
	branchClient, branchTable, err := NewBtTable(
		ctx, s.metadata.BtProject, s.metadata.BranchBtInstance, branchTableName)
	if err != nil {
		log.Printf("Failed to udpate branch cache Bigtable client: %v", err)
		return
	}
	s.branchBtClient = branchClient
	s.btTables[branchBtIndex] = branchTable
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
func NewMetadata(bqDataset, btProject, branchInstance string) (*Metadata, error) {
	_, filename, _, _ := runtime.Caller(0)
	subTypeMap, err := translator.GetSubTypeMap(
		path.Join(path.Dir(filename), "../translator/table_types.json"))
	if err != nil {
		return nil, err
	}
	schemaPath := path.Join(path.Dir(filename), "../translator/mapping/")
	files, err := ioutil.ReadDir(schemaPath)
	if err != nil {
		return nil, err
	}
	mappings := []*base.Mapping{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".mcf") {
			mappingStr, err := ioutil.ReadFile(filepath.Join(schemaPath, f.Name()))
			if err != nil {
				return nil, err
			}
			mapping, err := translator.ParseMapping(string(mappingStr), bqDataset)
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, mapping...)
		}
	}
	outArcInfo := map[string]map[string][]translator.OutArcInfo{}
	inArcInfo := map[string][]translator.InArcInfo{}
	return &Metadata{
		mappings, outArcInfo, inArcInfo, subTypeMap, bqDataset, btProject, branchInstance}, nil
}

// NewBtTable creates a new bigtable.Table instance.
func NewBtTable(
	ctx context.Context, projectID, instanceID, tableID string) (
	*bigtable.Client, *bigtable.Table, error) {
	btClient, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		return nil, nil, err
	}
	return btClient, btClient.Open(tableID), nil
}

// SubscribeBranchCacheUpdate subscribe server for branch cache update.
func (s *Server) SubscribeBranchCacheUpdate(
	ctx context.Context, pubsubProjectID, branchCacheBucket, subscriberPrefix,
	pubsubTopic string) error {
	// Cloud PubSub receiver when branch cache is updated.
	pubsubClient, err := pubsub.NewClient(ctx, pubsubProjectID)
	if err != nil {
		return err
	}
	// Always create a new subscriber with default expiration date of 2 days.
	subID := subscriberPrefix + util.RandomString()
	expiration, _ := time.ParseDuration("36h")
	retention, _ := time.ParseDuration("24h")
	sub, err := pubsubClient.CreateSubscription(ctx, subID,
		pubsub.SubscriptionConfig{
			Topic:             pubsubClient.Topic(pubsubTopic),
			ExpirationPolicy:  expiration,
			RetentionDuration: retention,
		})
	if err != nil {
		return err
	}
	fmt.Printf("Subscriber ID: %s\n", subID)
	// Start the receiver in a goroutine.
	go func() {
		err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			branchTableName := string(msg.Data)
			msg.Ack()
			fmt.Printf("Subscriber action: use branch cache %s\n", branchTableName)
			s.updateBranchTable(ctx, branchTableName)
		})
		if err != nil {
			log.Printf("Cloud pubsub receive: %v", err)
		}
	}()
	return nil
}

// NewServer creates a new server instance.
func NewServer(
	bqClient *bigquery.Client,
	branchBtClient *bigtable.Client,
	btTables []*bigtable.Table,
	metadata *Metadata) *Server {
	s := &Server{}
	s.bqClient = bqClient
	s.branchBtClient = branchBtClient
	s.btTables = btTables
	s.metadata = metadata
	return s
}
