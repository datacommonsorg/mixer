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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"path"
	"runtime"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/sparql"
	"github.com/datacommonsorg/mixer/store"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	st         store.Interface
	subTypeMap map[string]string
}

func (s *server) Query(ctx context.Context, in *pb.QueryRequest) (*pb.QueryResponse, error) {
	out := pb.QueryResponse{}
	if err := s.st.Query(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) QueryPost(ctx context.Context, in *pb.QueryRequest) (*pb.QueryResponse, error) {
	out := pb.QueryResponse{}
	if err := s.st.Query(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPropertyLabels(ctx context.Context,
	in *pb.GetPropertyLabelsRequest) (*pb.GetPropertyLabelsResponse, error) {
	if len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPropertyLabelsResponse{}
	if err := s.st.GetPropertyLabels(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPropertyLabelsPost(ctx context.Context,
	in *pb.GetPropertyLabelsRequest) (*pb.GetPropertyLabelsResponse, error) {
	if len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPropertyLabelsResponse{}
	if err := s.st.GetPropertyLabels(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPropertyValues(ctx context.Context,
	in *pb.GetPropertyValuesRequest) (*pb.GetPropertyValuesResponse, error) {
	if in.GetProperty() == "" || len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPropertyValuesResponse{}
	if err := s.st.GetPropertyValues(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPropertyValuesPost(ctx context.Context,
	in *pb.GetPropertyValuesRequest) (*pb.GetPropertyValuesResponse, error) {
	if in.GetProperty() == "" || len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPropertyValuesResponse{}
	if err := s.st.GetPropertyValues(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetTriples(ctx context.Context,
	in *pb.GetTriplesRequest) (*pb.GetTriplesResponse, error) {
	if len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("must provide DCIDs")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetTriplesResponse{}
	if err := s.st.GetTriples(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetTriplesPost(ctx context.Context,
	in *pb.GetTriplesRequest) (*pb.GetTriplesResponse, error) {
	if len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("must provide DCIDs")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetTriplesResponse{}
	if err := s.st.GetTriples(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPopObs(ctx context.Context,
	in *pb.GetPopObsRequest) (*pb.GetPopObsResponse, error) {
	if in.GetDcid() == "" {
		return nil, fmt.Errorf("must provide a DCID")
	}
	if !util.CheckValidDCIDs([]string{in.GetDcid()}) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPopObsResponse{}
	if err := s.st.GetPopObs(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPlaceObs(ctx context.Context,
	in *pb.GetPlaceObsRequest) (*pb.GetPlaceObsResponse, error) {
	if in.GetPlaceType() == "" || in.GetPopulationType() == "" ||
		in.GetObservationDate() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetPlaceObsResponse{}
	if err := s.st.GetPlaceObs(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetObsSeries(ctx context.Context,
	in *pb.GetObsSeriesRequest) (*pb.GetObsSeriesResponse, error) {
	if in.GetPlace() == "" || in.GetPopulationType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetObsSeriesResponse{}
	if err := s.st.GetObsSeries(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetStats(ctx context.Context,
	in *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	if len(in.GetPlace()) == 0 || in.GetStatsVar() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetStatsResponse{}
	if err := s.st.GetStats(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPopCategory(ctx context.Context,
	in *pb.GetPopCategoryRequest) (*pb.GetPopCategoryResponse, error) {
	if in.GetPlaceType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetPopCategoryResponse{}
	if err := s.st.GetPopCategory(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPopulations(ctx context.Context,
	in *pb.GetPopulationsRequest) (*pb.GetPopulationsResponse, error) {
	if len(in.GetDcids()) == 0 || in.GetPopulationType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPopulationsResponse{}
	if err := s.st.GetPopulations(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetObservations(ctx context.Context,
	in *pb.GetObservationsRequest) (*pb.GetObservationsResponse, error) {
	// TODO: Add checks for empty in.GetStatType().
	if len(in.GetDcids()) == 0 || in.GetMeasuredProperty() == "" ||
		in.GetObservationDate() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetObservationsResponse{}
	if err := s.st.GetObservations(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest) (*pb.GetPlacesInResponse, error) {
	if len(in.GetDcids()) == 0 || in.GetPlaceType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPlacesInResponse{}
	if err := s.st.GetPlacesIn(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetPlacesInPost(ctx context.Context,
	in *pb.GetPlacesInRequest) (*pb.GetPlacesInResponse, error) {
	if len(in.GetDcids()) == 0 || in.GetPlaceType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetPlacesInResponse{}
	if err := s.st.GetPlacesIn(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetRelatedPlaces(ctx context.Context,
	in *pb.GetRelatedPlacesRequest) (*pb.GetRelatedPlacesResponse, error) {
	if len(in.GetDcids()) == 0 || in.GetPopulationType() == "" ||
		in.GetMeasuredProperty() == "" || in.GetStatType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetRelatedPlacesResponse{}
	if err := s.st.GetRelatedPlaces(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetInterestingPlaceAspects(ctx context.Context,
	in *pb.GetInterestingPlaceAspectsRequest) (*pb.GetInterestingPlaceAspectsResponse, error) {
	if len(in.GetDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	out := pb.GetInterestingPlaceAspectsResponse{}
	if err := s.st.GetInterestingPlaceAspects(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) GetChartData(ctx context.Context,
	in *pb.GetChartDataRequest) (*pb.GetChartDataResponse, error) {
	if len(in.GetKeys()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetChartDataResponse{}
	if err := s.st.GetChartData(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *server) Translate(ctx context.Context,
	in *pb.TranslateRequest) (*pb.TranslateResponse, error) {
	if in.GetSchemaMapping() == "" || in.GetSparql() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.TranslateResponse{}
	mappings, err := translator.ParseMapping(in.GetSchemaMapping())
	if err != nil {
		return nil, err
	}
	nodes, queries, opts, err := sparql.ParseQuery(in.GetSparql())
	if err != nil {
		return nil, err
	}
	trans, err := translator.Translate(mappings, nodes, queries, s.subTypeMap, opts)
	if err != nil {
		return nil, err
	}
	log.Println(trans)
	out.Sql = trans.SQL
	translation, err := json.MarshalIndent(trans, "", "  ")
	if err != nil {
		return nil, err
	}
	out.Translation = string(translation)
	return &out, nil
}

func (s *server) Search(
	ctx context.Context, in *pb.SearchRequest) (*pb.SearchResponse, error) {
	out := pb.SearchResponse{}
	if err := s.st.Search(ctx, in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Server wraps gRpc go server.
type Server struct {
	Addr string
	Lis  net.Listener
	Srv  *grpc.Server
}

// NewServer creates a new Server instance given config parameters.
func NewServer(
	port, bqDataset, btTable, btProject, btInstance, projectID, schemaPath string,
	branchCache bool,
) (*Server, error) {
	l, err := net.Listen("tcp", port)
	if err != nil {
		return nil, err
	}
	log.Println("Now listening to port ", port)

	_, filename, _, _ := runtime.Caller(0)
	subTypeMap, err := translator.GetSubTypeMap(
		path.Join(path.Dir(filename), "../translator/table_types.json"))
	if err != nil {
		log.Fatalf("translator.GetSubTypeMap() = %v", err)
	}

	containedIn, err := util.GetContainedIn(
		path.Join(path.Dir(filename), "../type_relation.json"))
	if err != nil {
		log.Fatalf("util.GetContainedIn() = %v", err)
	}

	st, err := store.NewStore(
		bqDataset, btTable, btProject, btInstance, projectID,
		schemaPath, subTypeMap, containedIn, branchCache)
	if err != nil {
		log.Fatalf("Failed to create store for %s, %s, %s, %s, %s: %v",
			bqDataset, btTable, btProject, btInstance, projectID, err)
	}

	srv := grpc.NewServer()
	pb.RegisterMixerServer(srv, &server{st, subTypeMap})
	// Register reflection service on gRPC server.
	reflection.Register(srv)
	log.Println("Mixer server ready to serve!")
	return &Server{
		Addr: l.Addr().String(),
		Lis:  l,
		Srv:  srv,
	}, nil
}
