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
	"encoding/json"
	"os"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Query implements API for Mixer.Query.
func (s *Server) Query(ctx context.Context, in *pb.QueryRequest) (
	*pb.QueryResponse, error,
) {
	return translator.Query(ctx, in, s.metadata, s.store)
}

// GetStats implements API for Mixer.GetStats.
// Endpoint: /stat/set/series
// Endpoint: /bulk/stats
func (s *Server) GetStats(ctx context.Context, in *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	return stat.GetStats(ctx, in, s.store)
}

// GetRelatedLocations implements API for Mixer.GetRelatedLocations.
func (s *Server) GetRelatedLocations(
	ctx context.Context, in *pb.GetRelatedLocationsRequest,
) (*pb.GetRelatedLocationsResponse, error) {
	localResp, err := place.GetRelatedLocations(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	if len(localResp.GetData()) == 0 &&
		s.metadata.RemoteMixerDomain != "" {
		remoteResp := &pb.GetRelatedLocationsResponse{}
		if err := util.FetchRemote(
			ctx, s.metadata, s.httpClient, "/v1/place/related", in, remoteResp); err != nil {
			return nil, err
		}
		return remoteResp, nil
	}
	return localResp, nil
}

// getLocationsRankings fetches local location rankings, falling back to remote mixer if local data is empty.
func (s *Server) getLocationsRankings(
	ctx context.Context, in *pb.GetLocationsRankingsRequest, remoteAPIPath string,
) (*pb.GetLocationsRankingsResponse, error) {
	localResp, err := place.GetLocationsRankings(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	if len(localResp.GetData()) == 0 &&
		s.metadata.RemoteMixerDomain != "" {
		remoteResp := &pb.GetLocationsRankingsResponse{}
		if err := util.FetchRemote(
			ctx, s.metadata, s.httpClient, remoteAPIPath, in, remoteResp); err != nil {
			return nil, err
		}
		return remoteResp, nil
	}
	return localResp, nil
}

// GetLocationsRankings implements API for Mixer.GetLocationsRankings.
func (s *Server) GetLocationsRankings(
	ctx context.Context, in *pb.GetLocationsRankingsRequest,
) (*pb.GetLocationsRankingsResponse, error) {
	return s.getLocationsRankings(ctx, in, "/v1/place/ranking")
}

// GetVersion implements API for Mixer.GetVersion.
func (s *Server) GetVersion(
	ctx context.Context, in *pb.GetVersionRequest,
) (*pb.GetVersionResponse, error) {
	tableNames := []string{}
	if s.store.BtGroup != nil {
		tableNames = s.store.BtGroup.TableNames()
	}
	featureFlagsJson, err := json.Marshal(s.flags)
	if err != nil {
		return nil, err
	}
	var spannerStalenessTimestamp *timestamppb.Timestamp
	if s.spannerStalenessTimestampProvider != nil {
		if timestamp, err := s.spannerStalenessTimestampProvider.SpannerStalenessTimestamp(); err == nil {
			spannerStalenessTimestamp = timestamppb.New(timestamp)
		}
	}
	return &pb.GetVersionResponse{
		Tables:                    tableNames,
		Bigquery:                  s.metadata.BigQueryDataset,
		GitHash:                   os.Getenv("MIXER_HASH"),
		RemoteMixerDomain:         s.metadata.RemoteMixerDomain,
		FeatureFlags:              string(featureFlagsJson),
		DataSourceIds:             s.dispatcher.GetSources(),
		SpannerStalenessTimestamp: spannerStalenessTimestamp,
	}, nil
}

// ResolveIds implements API for Mixer.ResolveIds.
func (s *Server) ResolveIds(
	ctx context.Context, in *pb.ResolveIdsRequest,
) (*pb.ResolveIdsResponse, error) {
	return recon.ResolveIds(ctx, in, s.store)
}

// ResolveEntities implements API for Mixer.ResolveEntities.
func (s *Server) ResolveEntities(
	ctx context.Context, in *pb.ResolveEntitiesRequest,
) (*pb.ResolveEntitiesResponse, error) {
	return recon.ResolveEntities(ctx, in, s.store)
}

// ResolveCoordinates implements API for Mixer.ResolveCoordinates.
func (s *Server) ResolveCoordinates(
	ctx context.Context, in *pb.ResolveCoordinatesRequest,
) (*pb.ResolveCoordinatesResponse, error) {
	return recon.ResolveCoordinates(ctx, in, s.store)
}

// BulkFindEntities implements API for Mixer.BulkFindEntities.
func (s *Server) BulkFindEntities(
	ctx context.Context, in *pb.BulkFindEntitiesRequest,
) (*pb.BulkFindEntitiesResponse, error) {
	return recon.BulkFindEntities(ctx, in, s.store, s.mapsClient)
}

// GetImportTableData implements API for Mixer.GetImportTableData
func (s *Server) GetImportTableData(
	ctx context.Context, in *pb.GetImportTableDataRequest,
) (*pb.GetImportTableDataResponse, error) {
	response, err := sqlquery.GetImportTableData(ctx, &s.store.SQLClient)
	return response, err
}
