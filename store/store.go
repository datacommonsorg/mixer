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

// Package store is a library for querying datacommons backend storage.
package store

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"

	"github.com/datacommonsorg/mixer/base"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"google.golang.org/api/option"
)

// Interface exposes the database access for mixer.
type Interface interface {
	Query(ctx context.Context,
		in *pb.QueryRequest, out *pb.QueryResponse) error

	GetPropertyLabels(ctx context.Context,
		in *pb.GetPropertyLabelsRequest, out *pb.GetPropertyLabelsResponse) error

	GetPropertyValues(ctx context.Context,
		in *pb.GetPropertyValuesRequest, out *pb.GetPropertyValuesResponse) error

	GetTriples(ctx context.Context,
		in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error

	GetPopObs(ctx context.Context,
		in *pb.GetPopObsRequest, out *pb.GetPopObsResponse) error

	GetPlaceObs(ctx context.Context,
		in *pb.GetPlaceObsRequest, out *pb.GetPlaceObsResponse) error

	GetPopulations(ctx context.Context,
		in *pb.GetPopulationsRequest, out *pb.GetPopulationsResponse) error

	GetObservations(ctx context.Context,
		in *pb.GetObservationsRequest, out *pb.GetObservationsResponse) error

	GetPlacesIn(ctx context.Context,
		in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error

	GetPlaceKML(ctx context.Context,
		in *pb.GetPlaceKMLRequest, out *pb.GetPlaceKMLResponse) error
}

type store struct {
	bqDb        string
	bqClient    *bigquery.Client
	bqMapping   []*base.Mapping
	outArcInfo  map[string]map[string][]translator.OutArcInfo
	inArcInfo   map[string][]translator.InArcInfo
	subTypeMap  map[string]string
	containedIn map[util.TypePair][]string
	btClient    *bigtable.Client
}

// NewStore returns an implementation of Interface backed by BigQuery and BigTable.
func NewStore(ctx context.Context, bqDb, projectID, schemaPath string,
	subTypeMap map[string]string, containedIn map[util.TypePair][]string) (Interface, error) {
	// Initialize BigQuery client
	bqOptions := []option.ClientOption{}
	bqClient, err := bigquery.NewClient(ctx, projectID, bqOptions...)
	if err != nil {
		return nil, err
	}

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
			mapping, err := translator.ParseMapping(string(mappingStr))
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, mapping...)
		}
	}

	outArcInfo := map[string]map[string][]translator.OutArcInfo{}
	inArcInfo := map[string][]translator.InArcInfo{}

	btOptions := []option.ClientOption{}
	btClient, err := bigtable.NewClient(ctx, util.BtProject, util.BtInstance, btOptions...)
	if err != nil {
		return nil, err
	}

	return &store{bqDb, bqClient, mappings, outArcInfo, inArcInfo,
		subTypeMap, containedIn, btClient}, nil
}
