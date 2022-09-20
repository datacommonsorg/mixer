// Copyright 2021 Google LLC
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
	"github.com/datacommonsorg/mixer/internal/server/convert"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/search"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/server/v0/internalbio"
	"github.com/datacommonsorg/mixer/internal/server/v0/internalplace"
	"github.com/datacommonsorg/mixer/internal/server/v0/placemetadata"
	"github.com/datacommonsorg/mixer/internal/server/v0/placestatvar"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertylabel"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertyvalue"
	"github.com/datacommonsorg/mixer/internal/server/v0/statpoint"
	"github.com/datacommonsorg/mixer/internal/server/v0/statset"
	"github.com/datacommonsorg/mixer/internal/server/v0/statvarpath"
	"github.com/datacommonsorg/mixer/internal/server/v0/statvarsummary"
	"github.com/datacommonsorg/mixer/internal/server/v0/triple"
	"google.golang.org/protobuf/encoding/protojson"
)

// Translate implements API for Mixer.Translate.
func (s *Server) Translate(ctx context.Context, in *pb.TranslateRequest) (
	*pb.TranslateResponse, error,
) {
	return translator.Translate(ctx, in, s.metadata)
}

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

// GetStatSet implements API for Mixer.GetStatSet.
// Endpoint: /stat/set
func (s *Server) GetStatSet(ctx context.Context, in *pb.GetStatSetRequest) (
	*pb.GetStatSetResponse, error,
) {
	return statset.GetStatSet(ctx, in, s.store)
}

// GetStatSetWithinPlace implements API for Mixer.GetStatSetWithinPlace.
// Endpoint: /stat/set/within-place
func (s *Server) GetStatSetWithinPlace(
	ctx context.Context, in *pb.GetStatSetWithinPlaceRequest,
) (*pb.GetStatSetResponse, error) {
	return statset.GetStatSetWithinPlace(ctx, in, s.store)
}

// GetStatSetWithinPlaceAll implements API for Mixer.GetStatSetWithinPlaceAll.
// Endpoint: /stat/set/within-place/all
func (s *Server) GetStatSetWithinPlaceAll(
	ctx context.Context, in *pb.GetStatSetWithinPlaceRequest,
) (*pb.GetStatSetAllResponse, error) {
	return statset.GetStatSetWithinPlaceAll(ctx, in, s.store)
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

// GetStatSetSeries implements API for Mixer.GetStatSetSeries.
// Endpoint: /v1/stat/set/series
func (s *Server) GetStatSetSeries(
	ctx context.Context, in *pb.GetStatSetSeriesRequest,
) (*pb.GetStatSetSeriesResponse, error) {
	return stat.GetStatSetSeries(ctx, in, s.store)
}

// GetStatSetSeriesWithinPlace implements API for Mixer.GetStatSetSeriesWithinPlace.
// Endpoint: /v1/stat/set/series/within-place
func (s *Server) GetStatSetSeriesWithinPlace(
	ctx context.Context, in *pb.GetStatSetSeriesWithinPlaceRequest,
) (*pb.GetStatSetSeriesResponse, error) {
	return stat.GetStatSetSeriesWithinPlace(ctx, in, s.store)
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
	return place.GetRelatedLocations(ctx, in, s.store)
}

// GetLocationsRankings implements API for Mixer.GetLocationsRankings.
func (s *Server) GetLocationsRankings(
	ctx context.Context, in *pb.GetLocationsRankingsRequest,
) (*pb.GetLocationsRankingsResponse, error) {
	return place.GetLocationsRankings(ctx, in, s.store)
}

// GetPlaceMetadata implements API for Mixer.GetPlaceMetadata.
func (s *Server) GetPlaceMetadata(
	ctx context.Context, in *pb.GetPlaceMetadataRequest,
) (*pb.GetPlaceMetadataResponse, error) {
	return placemetadata.GetPlaceMetadata(ctx, in, s.store)
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
	return statvar.GetEntityStatVarsUnionV1(ctx, in, s.store)
}

// GetStatVarGroup implements API for Mixer.GetStatVarGroup.
func (s *Server) GetStatVarGroup(
	ctx context.Context, in *pb.GetStatVarGroupRequest,
) (*pb.StatVarGroups, error) {
	return statvar.GetStatVarGroup(ctx, in, s.store, s.cache)
}

// GetStatVarGroupNode implements API for Mixer.GetStatVarGroupNode.
func (s *Server) GetStatVarGroupNode(
	ctx context.Context, in *pb.GetStatVarGroupNodeRequest,
) (*pb.StatVarGroupNode, error) {
	return statvar.GetStatVarGroupNode(ctx, in, s.store, s.cache)
}

// GetStatVarPath implements API for Mixer.GetStatVarPath.
func (s *Server) GetStatVarPath(
	ctx context.Context, in *pb.GetStatVarPathRequest,
) (*pb.GetStatVarPathResponse, error) {
	return statvarpath.GetStatVarPath(ctx, in, s.store, s.cache)
}

// GetStatVarSummary implements API for Mixer.GetStatVarSummary.
func (s *Server) GetStatVarSummary(
	ctx context.Context, in *pb.GetStatVarSummaryRequest,
) (*pb.GetStatVarSummaryResponse, error) {
	return statvarsummary.GetStatVarSummary(ctx, in, s.store)
}

// GetStatVarMatch implements API for Mixer.GetStatVarMatch.
func (s *Server) GetStatVarMatch(
	ctx context.Context, in *pb.GetStatVarMatchRequest,
) (*pb.GetStatVarMatchResponse, error) {
	return statvar.GetStatVarMatch(ctx, in, s.store, s.cache)
}

// SearchStatVar implements API for Mixer.SearchStatVar.
func (s *Server) SearchStatVar(
	ctx context.Context, in *pb.SearchStatVarRequest,
) (*pb.SearchStatVarResponse, error) {
	return statvar.SearchStatVar(ctx, in, s.store, s.cache)
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

// GetPlacePageData implements API for Mixer.GetPlacePageData.
//
// TODO(shifucun):For each related place, it is supposed to have dcid, name and
// population but it's not complete now as the client in most cases only requires
// the dcid. Should consider have the full name, even with parent place
// abbreviations like "CA" filled in here so the client won't bother to fetch
// those again.
func (s *Server) GetPlacePageData(
	ctx context.Context, in *pb.GetPlacePageDataRequest,
) (*pb.GetPlacePageDataResponse, error) {
	return internalplace.GetPlacePageData(ctx, in, s.store)
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
	return &pb.GetVersionResponse{
		Store:    s.metadata.CoreBigtableProject,
		BigQuery: s.metadata.BigQueryDataset,
		Tables:   s.store.BtGroup.TableNames(),
		GitHash:  os.Getenv("MIXER_HASH"),
	}, nil
}

// ResolveIds implements API for Recon.ResolveIds.
func (s *Server) ResolveIds(
	ctx context.Context, in *pb.ResolveIdsRequest,
) (*pb.ResolveIdsResponse, error) {
	return recon.ResolveIds(ctx, in, s.store)
}

// ResolveEntities implements API for ReconServer.ResolveEntities.
func (s *Server) ResolveEntities(
	ctx context.Context, in *pb.ResolveEntitiesRequest,
) (*pb.ResolveEntitiesResponse, error) {
	return recon.ResolveEntities(ctx, in, s.store)
}

// ResolveCoordinates implements API for ReconServer.ResolveCoordinates.
func (s *Server) ResolveCoordinates(
	ctx context.Context, in *pb.ResolveCoordinatesRequest,
) (*pb.ResolveCoordinatesResponse, error) {
	return recon.ResolveCoordinates(ctx, in, s.store)
}

// CompareEntities implements API for Recon.CompareEntities.
func (s *Server) CompareEntities(
	ctx context.Context, in *pb.CompareEntitiesRequest,
) (
	*pb.CompareEntitiesResponse, error) {
	// TODO(spaceenter): Implement.
	return &pb.CompareEntitiesResponse{
		Comparisons: []*pb.CompareEntitiesResponse_Comparison{
			{
				SourceIds:   []string{"aaa", "bbb"},
				Probability: 0.67,
			},
		},
	}, nil
}
