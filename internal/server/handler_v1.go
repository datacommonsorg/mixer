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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/server/statvar/search"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/server/v1/event"
	"github.com/datacommonsorg/mixer/internal/server/v1/info"
	"github.com/datacommonsorg/mixer/internal/server/v1/observationdates"
	"github.com/datacommonsorg/mixer/internal/server/v1/observationexistence"
	"github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/server/v1/page"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/server/v1/triples"
	"github.com/datacommonsorg/mixer/internal/server/v1/variable"
	"github.com/datacommonsorg/mixer/internal/server/v1/variables"
	"github.com/datacommonsorg/mixer/internal/util"
)

const foldedSvgRoot = "dc/g/Folded_Root"

// QueryV1 implements API for Mixer.Query.
func (s *Server) QueryV1(
	ctx context.Context, in *pb.QueryRequest,
) (*pb.QueryResponse, error) {
	return translator.Query(ctx, in, s.metadata, s.store)
}

// Properties implements API for mixer.Properties.
func (s *Server) Properties(
	ctx context.Context, in *pbv1.PropertiesRequest,
) (*pbv1.PropertiesResponse, error) {
	return properties.Properties(ctx, in, s.store)
}

// BulkProperties implements API for mixer.BulkProperties.
func (s *Server) BulkProperties(
	ctx context.Context, in *pbv1.BulkPropertiesRequest,
) (*pbv1.BulkPropertiesResponse, error) {
	return properties.BulkProperties(ctx, in, s.store)
}

// PropertyValues implements API for mixer.PropertyValues.
func (s *Server) PropertyValues(
	ctx context.Context, in *pbv1.PropertyValuesRequest,
) (*pbv1.PropertyValuesResponse, error) {
	return propertyvalues.PropertyValues(ctx, in, s.store)
}

// LinkedPropertyValues implements API for mixer.LinkedPropertyValues.
func (s *Server) LinkedPropertyValues(
	ctx context.Context, in *pbv1.LinkedPropertyValuesRequest,
) (*pbv1.PropertyValuesResponse, error) {
	return propertyvalues.LinkedPropertyValues(ctx, in, s.store)
}

// BulkLinkedPropertyValues implements API for mixer.BulkLinkedPropertyValues.
func (s *Server) BulkLinkedPropertyValues(
	ctx context.Context, in *pbv1.BulkLinkedPropertyValuesRequest,
) (*pbv1.BulkPropertyValuesResponse, error) {
	return propertyvalues.BulkLinkedPropertyValues(ctx, in, s.store)
}

// BulkPropertyValues implements API for mixer.BulkPropertyValues.
func (s *Server) BulkPropertyValues(
	ctx context.Context, in *pbv1.BulkPropertyValuesRequest,
) (*pbv1.BulkPropertyValuesResponse, error) {
	return propertyvalues.BulkPropertyValues(ctx, in, s.store)
}

// Triples implements API for mixer.Triples.
func (s *Server) Triples(
	ctx context.Context, in *pbv1.TriplesRequest,
) (*pbv1.TriplesResponse, error) {
	return triples.Triples(ctx, in, s.store, s.metadata)
}

// BulkTriples implements API for mixer.BulkTriples.
func (s *Server) BulkTriples(
	ctx context.Context, in *pbv1.BulkTriplesRequest,
) (*pbv1.BulkTriplesResponse, error) {
	return triples.BulkTriples(ctx, in, s.store, s.metadata)
}

// Variables implements API for mixer.Variables.
func (s *Server) Variables(
	ctx context.Context, in *pbv1.VariablesRequest,
) (*pbv1.VariablesResponse, error) {
	return variables.Variables(ctx, in, s.store)
}

// BulkVariables implements API for mixer.BulkVariables.
func (s *Server) BulkVariables(
	ctx context.Context, in *pbv1.BulkVariablesRequest,
) (*pbv1.BulkVariablesResponse, error) {
	return variables.BulkVariables(ctx, in, s.store)
}

// PlaceInfo implements API for mixer.PlaceInfo.
func (s *Server) PlaceInfo(
	ctx context.Context, in *pbv1.PlaceInfoRequest,
) (*pbv1.PlaceInfoResponse, error) {
	return info.PlaceInfo(ctx, in, s.store)
}

// BulkPlaceInfo implements API for mixer.BulkPlaceInfo.
func (s *Server) BulkPlaceInfo(
	ctx context.Context, in *pbv1.BulkPlaceInfoRequest,
) (*pbv1.BulkPlaceInfoResponse, error) {
	localResp, err := info.BulkPlaceInfo(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	// Store result of request nodes in a map, keyed by node DCID.
	keyedInfo := map[string]*pbv1.PlaceInfoResponse{}
	for _, item := range localResp.Data {
		keyedInfo[item.GetNode()] = item
	}
	if s.metadata.RemoteMixerDomain != "" {
		// From local response, find nodes that has no place info, fetch the
		// info again from remote mixer.
		in.Nodes = []string{}
		for _, item := range localResp.Data {
			if item.Info == nil {
				in.Nodes = append(in.Nodes, item.Node)
			}
		}
		if len(in.Nodes) > 0 {
			remoteResp := &pbv1.BulkPlaceInfoResponse{}
			if err := util.FetchRemote(
				s.metadata,
				s.httpClient,
				"/v1/bulk/info/place",
				in,
				remoteResp,
			); err != nil {
				return nil, err
			}
			for _, item := range remoteResp.Data {
				keyedInfo[item.GetNode()] = item
			}
		}
	}
	result := &pbv1.BulkPlaceInfoResponse{
		Data: []*pbv1.PlaceInfoResponse{},
	}
	for _, item := range keyedInfo {
		result.Data = append(result.Data, item)
	}
	sort.Slice(result.Data, func(i, j int) bool {
		return result.Data[i].Node < result.Data[j].Node
	})
	return result, nil
}

// VariableInfo implements API for mixer.VariableInfo.
func (s *Server) VariableInfo(
	ctx context.Context, in *pbv1.VariableInfoRequest,
) (*pbv1.VariableInfoResponse, error) {
	return info.VariableInfo(ctx, in, s.store)
}

// BulkVariableInfo implements API for mixer.BulkVariableInfo.
func (s *Server) BulkVariableInfo(
	ctx context.Context, in *pbv1.BulkVariableInfoRequest,
) (*pbv1.BulkVariableInfoResponse, error) {
	localResp, err := info.BulkVariableInfo(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	keyedInfo := map[string]*pbv1.VariableInfoResponse{}
	for _, item := range localResp.Data {
		keyedInfo[item.GetNode()] = item
	}
	if s.metadata.RemoteMixerDomain != "" {
		in.Nodes = []string{}
		for _, item := range localResp.Data {
			if item.Info == nil {
				in.Nodes = append(in.Nodes, item.Node)
			}
		}
		if len(in.Nodes) > 0 {
			remoteResp := &pbv1.BulkVariableInfoResponse{}
			if err := util.FetchRemote(
				s.metadata,
				s.httpClient,
				"/v1/bulk/info/variable",
				in,
				remoteResp,
			); err != nil {
				return nil, err
			}
			for _, item := range remoteResp.Data {
				keyedInfo[item.GetNode()] = item
			}
		}
	}
	result := &pbv1.BulkVariableInfoResponse{
		Data: []*pbv1.VariableInfoResponse{},
	}
	for _, item := range keyedInfo {
		result.Data = append(result.Data, item)
	}
	sort.Slice(result.Data, func(i, j int) bool {
		return result.Data[i].Node < result.Data[j].Node
	})
	return result, nil
}

// VariableGroupInfo implements API for mixer.VariableGroupInfo.
func (s *Server) VariableGroupInfo(
	ctx context.Context, in *pbv1.VariableGroupInfoRequest,
) (*pbv1.VariableGroupInfoResponse, error) {
	return info.VariableGroupInfo(ctx, in, s.store, s.cachedata.Load())
}

// BulkVariableGroupInfo implements API for mixer.BulkVariableGroupInfo.
func (s *Server) BulkVariableGroupInfo(
	ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest,
) (*pbv1.BulkVariableGroupInfoResponse, error) {
	localResp, err := info.BulkVariableGroupInfo(ctx, in, s.store, s.cachedata.Load())
	if err != nil {
		return nil, err
	}
	keyedInfo := map[string]*pbv1.VariableGroupInfoResponse{}
	for _, item := range localResp.Data {
		keyedInfo[item.GetNode()] = item
	}
	if s.metadata.RemoteMixerDomain != "" {
		queryFoldedRoot := false
		for i := range in.Nodes {
			if in.Nodes[i] == foldedSvgRoot {
				queryFoldedRoot = true
				in.Nodes[i] = hierarchy.SvgRoot
				break
			}
		}
		remoteResp := &pbv1.BulkVariableGroupInfoResponse{}
		if err := util.FetchRemote(
			s.metadata,
			s.httpClient,
			"/v1/bulk/info/variable-group",
			in,
			remoteResp,
		); err != nil {
			return nil, err
		}

		for _, remoteItem := range remoteResp.Data {
			n := remoteItem.GetNode()
			if s.metadata.FoldRemoteRootSvg && n == hierarchy.SvgRoot {
				if queryFoldedRoot {
					n = foldedSvgRoot
				} else {
					// When query the root, make a folded node that folds all the
					// top level svg in it.
					//
					// For example, dc/g/Root has two children svg: dc/g/Root_1,
					// dc/g/Root_2. Here adds dc/g/Folded_Root as an intermediate node,
					// then we have:
					// dc/g/Root -> dc/g/Folded_root -> [dc/g/Root_1, dc/g/Root_2]
					foldedSvg := &pb.StatVarGroupNode_ChildSVG{
						Id:                     foldedSvgRoot,
						DescendentStatVarCount: remoteItem.Info.DescendentStatVarCount,
						SpecializedEntity:      "Google",
					}
					// Decrease the count from block list svg.
					for _, child := range remoteItem.Info.ChildStatVarGroups {
						if _, ok := s.cachedata.Load().BlocklistSvgs()[child.Id]; ok {
							foldedSvg.DescendentStatVarCount -= child.DescendentStatVarCount
						}
					}
					if foldedSvg.DescendentStatVarCount < 0 {
						foldedSvg.DescendentStatVarCount = 0
					}
					remoteItem.Info.ChildStatVarGroups = []*pb.StatVarGroupNode_ChildSVG{foldedSvg}
				}
			}
			if _, ok := keyedInfo[n]; ok {
				keyedInfo[n].Info.ChildStatVarGroups = append(
					keyedInfo[n].Info.ChildStatVarGroups,
					remoteItem.Info.ChildStatVarGroups...,
				)
				if s.metadata.FoldRemoteRootSvg && n == hierarchy.SvgRoot {
					for _, item := range keyedInfo[n].Info.ChildStatVarGroups {
						item.SpecializedEntity = "Imported by " + item.SpecializedEntity
					}
				}
				keyedInfo[n].Info.ChildStatVars = append(
					keyedInfo[n].Info.ChildStatVars,
					remoteItem.Info.ChildStatVars...,
				)
				keyedInfo[n].Info.DescendentStatVarCount += remoteItem.Info.DescendentStatVarCount
			} else {
				keyedInfo[n] = remoteItem
			}
			// Remove all the block list svg from child svg.
			childSvg := []*pb.StatVarGroupNode_ChildSVG{}
			for _, child := range keyedInfo[n].Info.ChildStatVarGroups {
				_, ok := s.cachedata.Load().BlocklistSvgs()[child.Id]
				if ok {
					keyedInfo[n].Info.DescendentStatVarCount -= child.DescendentStatVarCount
				} else {
					childSvg = append(childSvg, child)
				}
			}
			if keyedInfo[n].Info.DescendentStatVarCount < 0 {
				keyedInfo[n].Info.DescendentStatVarCount = 0
			}
			keyedInfo[n].Info.ChildStatVarGroups = childSvg
		}
	}
	result := &pbv1.BulkVariableGroupInfoResponse{
		Data: []*pbv1.VariableGroupInfoResponse{},
	}
	for _, node := range keyedInfo {
		result.Data = append(result.Data, node)
	}
	sort.SliceStable(result.Data, func(i, j int) bool {
		return result.Data[i].Node < result.Data[j].Node
	})
	return result, nil
}

// ObservationsPoint implements API for mixer.ObservationsPoint.
func (s *Server) ObservationsPoint(
	ctx context.Context, in *pbv1.ObservationsPointRequest,
) (*pb.PointStat, error) {
	return observations.Point(ctx, in, s.store)
}

// BulkObservationsPoint implements API for mixer.BulkObservationsPoint.
func (s *Server) BulkObservationsPoint(
	ctx context.Context, in *pbv1.BulkObservationsPointRequest,
) (*pbv1.BulkObservationsPointResponse, error) {
	return observations.BulkPoint(ctx, in, s.store)
}

// BulkObservationsPointLinked implements API for mixer.BulkObservationsPointLinked.
func (s *Server) BulkObservationsPointLinked(
	ctx context.Context, in *pbv1.BulkObservationsPointLinkedRequest,
) (*pbv1.BulkObservationsPointResponse, error) {
	return observations.BulkPointLinked(ctx, in, s.store)
}

// ObservationsSeries implements API for mixer.ObservationsSeries.
func (s *Server) ObservationsSeries(
	ctx context.Context, in *pbv1.ObservationsSeriesRequest,
) (*pbv1.ObservationsSeriesResponse, error) {
	return observations.Series(ctx, in, s.store)
}

// BulkObservationsSeries implements API for mixer.BulkObservationsSeries.
func (s *Server) BulkObservationsSeries(
	ctx context.Context, in *pbv1.BulkObservationsSeriesRequest,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeries(ctx, in, s.store)
}

// BulkObservationsSeriesLinked implements API for mixer.BulkObservationsSeriesLinked.
func (s *Server) BulkObservationsSeriesLinked(
	ctx context.Context, in *pbv1.BulkObservationsSeriesLinkedRequest,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeriesLinked(ctx, in, s.store)
}

// BulkObservationDatesLinked implements API for mixer.BulkObservationDatesLinked.
func (s *Server) BulkObservationDatesLinked(
	ctx context.Context, in *pbv1.BulkObservationDatesLinkedRequest,
) (*pbv1.BulkObservationDatesLinkedResponse, error) {
	return observationdates.BulkObservationDatesLinked(ctx, in, s.store)
}

// BulkObservationExistence implements API for mixer.BulkObservationExistence.
func (s *Server) BulkObservationExistence(
	ctx context.Context, in *pbv1.BulkObservationExistenceRequest,
) (*pbv1.BulkObservationExistenceResponse, error) {
	return observationexistence.BulkObservationExistence(ctx, in, s.store)
}

// BioPage implements API for mixer.BioPage.
func (s *Server) BioPage(
	ctx context.Context, in *pbv1.BioPageRequest,
) (*pb.GraphNodes, error) {
	return page.BioPage(ctx, in, s.store)
}

// PlacePage implements API for mixer.PlacePage.
func (s *Server) PlacePage(ctx context.Context, in *pbv1.PlacePageRequest) (
	*pbv1.PlacePageResponse, error) {
	localResp, err := page.PlacePage(ctx, in, s.store)
	if err != nil {
		return nil, err
	}
	if len(localResp.GetStatVarSeries()) == 0 && s.metadata.RemoteMixerDomain != "" {
		remoteResp := &pbv1.PlacePageResponse{}
		if err := util.FetchRemote(
			s.metadata,
			s.httpClient,
			"/v1/internal/page/place",
			in,
			remoteResp,
		); err != nil {
			return nil, err
		}
		return remoteResp, nil
	}
	return localResp, nil
}

// VariableAncestors implements API for Mixer.VariableAncestors.
func (s *Server) VariableAncestors(
	ctx context.Context, in *pbv1.VariableAncestorsRequest,
) (*pbv1.VariableAncestorsResponse, error) {
	localResp, err := variable.Ancestors(ctx, in, s.store, s.cachedata.Load())
	if err != nil {
		return nil, err
	}
	if len(localResp.Ancestors) == 0 && s.metadata.RemoteMixerDomain != "" {
		remoteResp := &pbv1.VariableAncestorsResponse{}
		if err := util.FetchRemote(
			s.metadata,
			s.httpClient,
			"/v1/variable/ancestors",
			in,
			remoteResp,
		); err != nil {
			return nil, err
		}
		if s.metadata.FoldRemoteRootSvg {
			remoteResp.Ancestors = append(remoteResp.Ancestors, foldedSvgRoot)
		}
		return remoteResp, nil
	}
	return localResp, nil
}

// DerivedObservationsSeries implements API for mixer.ObservationsSeries.
func (s *Server) DerivedObservationsSeries(
	ctx context.Context, in *pbv1.DerivedObservationsSeriesRequest,
) (*pbv1.DerivedObservationsSeriesResponse, error) {
	return observations.DerivedSeries(ctx, in, s.store)
}

// EventCollection implements API for mixer.EventCollection.
func (s *Server) EventCollection(
	ctx context.Context, in *pbv1.EventCollectionRequest,
) (*pbv1.EventCollectionResponse, error) {
	return event.Collection(ctx, in, s.store)
}

// EventCollectionDate implements API for mixer.EventCollectionDate.
func (s *Server) EventCollectionDate(
	ctx context.Context, in *pbv1.EventCollectionDateRequest,
) (*pbv1.EventCollectionDateResponse, error) {
	return event.CollectionDate(ctx, in, s.store)
}

// RecognizePlaces implements API for Mixer.RecognizePlaces.
func (s *Server) RecognizePlaces(
	ctx context.Context, in *pb.RecognizePlacesRequest,
) (*pb.RecognizePlacesResponse, error) {
	return recon.RecognizePlaces(ctx, in, s.store, false)
}

// SearchStatVar implements API for Mixer.SearchStatVar.
func (s *Server) SearchStatVar(
	ctx context.Context, in *pb.SearchStatVarRequest,
) (*pb.SearchStatVarResponse, error) {
	localResp, err := search.SearchStatVar(ctx, in, s.store, s.cachedata.Load())
	if err != nil {
		return nil, err
	}
	if len(localResp.StatVars) == 0 && len(localResp.Matches) == 0 {
		if s.metadata.RemoteMixerDomain != "" {
			remoteResp := &pb.SearchStatVarResponse{}
			if err := util.FetchRemote(
				s.metadata,
				s.httpClient,
				"/v1/variable/search",
				in,
				remoteResp,
			); err != nil {
				return nil, err
			}
			return remoteResp, nil
		}
	}
	return localResp, nil
}
