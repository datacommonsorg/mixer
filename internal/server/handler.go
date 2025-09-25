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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/convert"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/search"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/server/v0/internalbio"
	"github.com/datacommonsorg/mixer/internal/server/v0/placestatvar"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertylabel"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertyvalue"
	"github.com/datacommonsorg/mixer/internal/server/v0/statpoint"
	"github.com/datacommonsorg/mixer/internal/server/v0/triple"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/encoding/protojson"
)

// Query implements API for Mixer.Query.
func (s *Server) Query(ctx context.Context, in *pb.QueryRequest) (
	*pb.QueryResponse, error,
) {
	return translator.Query(ctx, in, s.metadata, s.store)
}

// GetStatValue implements API for Mixer.GetStatValue.
// Endpoint: /stat (/stat/value)
func (s *Server) GetStatValue(ctx context.Context, in *pb.GetStatValueRequest) (
	*pb.GetStatValueResponse, error,
) {
	return statpoint.GetStatValue(ctx, in, s.store)
}

// GetStatSeries implements API for Mixer.GetStatSeries.
// Endpoint: /stat/series
// TODO(shifucun): consilidate and dedup the logic among these similar APIs.
func (s *Server) GetStatSeries(ctx context.Context, in *pb.GetStatSeriesRequest) (
	*pb.GetStatSeriesResponse, error,
) {
	return stat.GetStatSeries(ctx, in, s.store)
}

// GetStats implements API for Mixer.GetStats.
// Endpoint: /stat/set/series
// Endpoint: /bulk/stats
func (s *Server) GetStats(ctx context.Context, in *pb.GetStatsRequest,
) (*pb.GetStatsResponse, error) {
	return stat.GetStats(ctx, in, s.store)
}

// GetStatAll implements API for Mixer.GetStatAll.
// Endpoint: /stat/set/series/all
// Endpoint: /stat/all
func (s *Server) GetStatAll(ctx context.Context, in *pb.GetStatAllRequest,
) (*pb.GetStatAllResponse, error) {
	return stat.GetStatAll(ctx, in, s.store)
}

// GetStatDateWithinPlace implements API for Mixer.GetStatDateWithinPlace.
// Endpoint: /v1/stat/date/within-place
func (s *Server) GetStatDateWithinPlace(
	ctx context.Context, in *pb.GetStatDateWithinPlaceRequest,
) (*pb.GetStatDateWithinPlaceResponse, error) {
	return stat.GetStatDateWithinPlace(ctx, in, s.store)
}

// GetPlacesIn implements API for Mixer.GetPlacesIn.
func (s *Server) GetPlacesIn(ctx context.Context, in *pb.GetPlacesInRequest,
) (*pb.GetPlacesInResponse, error) {
	// Current places in response is not ideal.
	placesInData, err := placein.GetPlacesIn(ctx, s.store, in.GetDcids(), in.GetPlaceType())
	if err != nil {
		return nil, err
	}
	// (TODO): In next version of API, should simply return placesInData in the final response.
	result := []map[string]string{}
	parents := []string{}
	for dcid := range placesInData {
		parents = append(parents, dcid)
	}
	sort.Strings(parents)
	for _, parent := range parents {
		for _, child := range placesInData[parent] {
			result = append(result, map[string]string{"dcid": parent, "place": child})
		}
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetPlacesInResponse{Payload: string(jsonRaw)}, nil
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
			s.metadata, s.httpClient, "/v1/place/related", in, remoteResp); err != nil {
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
	localResp, err := place.GetLocationsRankings(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	if len(localResp.GetData()) == 0 &&
		s.metadata.RemoteMixerDomain != "" {
		remoteResp := &pb.GetLocationsRankingsResponse{}
		if err := util.FetchRemote(
			s.metadata, s.httpClient, "/v1/place/ranking", in, remoteResp); err != nil {
			return nil, err
		}
		return remoteResp, nil
	}
	return localResp, nil
}

// GetPlaceStatDateWithinPlace implements API for Mixer.GetPlaceStatDateWithinPlace.
// Endpoint: /place/stat/date/within-place
func (s *Server) GetPlaceStatDateWithinPlace(
	ctx context.Context, in *pb.GetPlaceStatDateWithinPlaceRequest,
) (*pb.GetPlaceStatDateWithinPlaceResponse, error) {
	return place.GetPlaceStatDateWithinPlace(ctx, in, s.store)
}

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
// TODO(shifucun): Migrate clients to use GetPlaceStatVars and deprecate this.
func (s *Server) GetPlaceStatsVar(
	ctx context.Context, in *pb.GetPlaceStatsVarRequest,
) (*pb.GetPlaceStatsVarResponse, error) {
	return statvar.GetPlaceStatsVar(ctx, in, s.store)
}

// GetPlaceStatVars implements API for Mixer.GetPlaceStatVars.
func (s *Server) GetPlaceStatVars(
	ctx context.Context, in *pb.GetPlaceStatVarsRequest,
) (*pb.GetPlaceStatVarsResponse, error) {
	return placestatvar.GetPlaceStatVars(ctx, in, s.store)
}

// GetEntityStatVarsUnionV1 implements API for Mixer.GetEntityStatVarsUnionV1.
func (s *Server) GetEntityStatVarsUnionV1(
	ctx context.Context, in *pb.GetEntityStatVarsUnionRequest,
) (*pb.GetEntityStatVarsUnionResponse, error) {
	return statvar.GetEntityStatVarsUnionV1(ctx, in, s.store, s.cachedata.Load())
}

// GetPropertyLabels implements API for Mixer.GetPropertyLabels.
func (s *Server) GetPropertyLabels(
	ctx context.Context, in *pb.GetPropertyLabelsRequest,
) (*pb.PayloadResponse, error) {
	resp, err := propertylabel.GetPropertyLabels(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	jsonRaw, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}
	// To make the API response backward compatible. This is to remove the outer
	// level `{\"data\":` and trailing `}`
	jsonRaw = jsonRaw[8 : len(jsonRaw)-1]

	return &pb.PayloadResponse{Payload: string(jsonRaw)}, nil
}

// GetPropertyValues implements API for Mixer.GetPropertyValues.
func (s *Server) GetPropertyValues(
	ctx context.Context, in *pb.GetPropertyValuesRequest,
) (*pb.PayloadResponse, error) {
	resp, err := propertyvalue.GetPropertyValues(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	jsonRaw, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}
	// To make the API response backward compatible. This is to remove the outer
	// level `{\"data\":` and trailing `}`
	jsonRaw = jsonRaw[8 : len(jsonRaw)-1]
	return &pb.PayloadResponse{Payload: string(jsonRaw)}, nil
}

// GetTriples implements API for Mixer.GetTriples.
func (s *Server) GetTriples(ctx context.Context, in *pb.GetTriplesRequest,
) (*pb.PayloadResponse, error) {
	resp, err := triple.GetTriples(ctx, in, s.store, s.metadata)
	if err != nil {
		return nil, err
	}
	result := convert.ToLegacyResult(resp)
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.PayloadResponse{Payload: string(jsonRaw)}, nil
}

// GetBioPageData implements API for Mixer.GetBioPageData.
func (s *Server) GetBioPageData(
	ctx context.Context, in *pb.GetBioPageDataRequest,
) (*pb.GraphNodes, error) {
	return internalbio.GetBioPageData(ctx, in, s.store)
}

// Search implements API for Mixer.Search.
func (s *Server) Search(
	ctx context.Context, in *pb.SearchRequest,
) (*pb.SearchResponse, error) {
	return search.Search(ctx, in, s.store.BqClient, s.metadata.BigQueryDataset)
}

// GetVersion implements API for Mixer.GetVersion.
func (s *Server) GetVersion(
	ctx context.Context, in *pb.GetVersionRequest,
) (*pb.GetVersionResponse, error) {
	tableNames := []string{}
	if s.store.BtGroup != nil {
		tableNames = s.store.BtGroup.TableNames()
	}
	return &pb.GetVersionResponse{
		Tables:            tableNames,
		Bigquery:          s.metadata.BigQueryDataset,
		GitHash:           os.Getenv("MIXER_HASH"),
		RemoteMixerDomain: s.metadata.RemoteMixerDomain,
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

// FindEntities implements API for Mixer.FindEntities.
func (s *Server) FindEntities(
	ctx context.Context, in *pb.FindEntitiesRequest,
) (*pb.FindEntitiesResponse, error) {
	return recon.FindEntities(ctx, in, s.store, s.mapsClient)
}

// BulkFindEntities implements API for Mixer.BulkFindEntities.
func (s *Server) BulkFindEntities(
	ctx context.Context, in *pb.BulkFindEntitiesRequest,
) (*pb.BulkFindEntitiesResponse, error) {
	return recon.BulkFindEntities(ctx, in, s.store, s.mapsClient)
}

// UpdateCache implements API for Mixer.UpdateCache
func (s *Server) UpdateCache(
	ctx context.Context, in *pb.UpdateCacheRequest,
) (*pb.UpdateCacheResponse, error) {
	newCache, err := cache.NewCache(ctx, s.store, *s.cachedata.Load().Options(), s.metadata)
	if err != nil {
		return nil, err
	}
	s.cachedata.Swap(newCache)
	return &pb.UpdateCacheResponse{}, err
}

// GetImportTableData implements API for Mixer.GetImportTableData
func (s *Server) GetImportTableData(
	ctx context.Context, in *pb.GetImportTableDataRequest,
) (*pb.GetImportTableDataResponse, error) {
	response, err := sqlquery.GetImportTableData(ctx, &s.store.SQLClient)
	return response, err
}
