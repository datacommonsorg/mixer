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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/store"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	bqDataset  = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	btTable    = flag.String("bt_table", "", "DataCommons Bigtable table.")
	btProject  = flag.String("bt_project", "", "GCP project containing the BigTable instance.")
	btInstance = flag.String("bt_instance", "", "BigTable instance.")
	projectID  = flag.String("project_id", "", "The cloud project to run the mixer instance.")
	schemaPath = flag.String("schema_path", "/mixer/config/mapping", "Path to the schema mapping directory.")
	port       = flag.String("port", ":12345", "Port on which to run the server.")
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

	if in.GetLimit() == 0 {
		in.Limit = 100
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
	if in.GetPlaceType() == "" || in.GetPopulationType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.GetPlaceObsResponse{}
	if err := s.st.GetPlaceObs(ctx, in, &out); err != nil {
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
	if len(in.GetDcids()) == 0 || in.GetMeasuredProperty() == "" ||
		in.GetObservationDate() == "" || in.GetStatsType() == "" {
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

func (s *server) Translate(ctx context.Context,
	in *pb.TranslateRequest) (*pb.TranslateResponse, error) {
	if in.GetSchemaMapping() == "" || in.GetDatalog() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}

	out := pb.TranslateResponse{}
	mappings, err := translator.ParseMapping(in.GetSchemaMapping())
	if err != nil {
		return nil, err
	}
	nodes, queries, err := translator.ParseQuery(in.GetDatalog())
	if err != nil {
		return nil, err
	}
	trans, err := translator.Translate(mappings, nodes, queries, s.subTypeMap)
	if err != nil {
		return nil, err
	}
	out.Sql = trans.SQL
	translation, err := json.MarshalIndent(trans, "", "  ")
	if err != nil {
		return nil, err
	}
	out.Translation = string(translation)
	return nil, nil
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	log.Println("Now listening to port ", *port)

	subTypeMap, err := translator.GetSubTypeMap("translator/table_types.json")
	if err != nil {
		log.Fatalf("translator.GetSubTypeMap() = %v", err)
	}

	containedIn, err := util.GetContainedIn("type_relation.json")
	if err != nil {
		log.Fatalf("util.GetContainedIn() = %v", err)
	}

	st, err := store.NewStore(
		ctx, *bqDataset, *btTable, *btProject, *btInstance, *projectID,
		*schemaPath, subTypeMap, containedIn)
	if err != nil {
		log.Fatalf("Failed to create store for %s, %s, %s, %s, %s: %s",
			*bqDataset, *btTable, *btProject, *btInstance, *projectID, err)
	}

	pb.RegisterMixerServer(s, &server{st, subTypeMap})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
