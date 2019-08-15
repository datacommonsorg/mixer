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

package handler

import (
	"context"
	"errors"
	"strings"

	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datacommonsorg/mixer/models"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/go-test/deep"
)

type mockStore struct{}

func (s *mockStore) Query(ctx context.Context, in *models.QueryRequest, out *models.QueryResponse) error {
	if in.Sparql == "SELECT*FROM" {
		out.Header = []string{"name", "gender"}
		out.Rows = []*models.Row{
			&models.Row{
				Cells: []*models.Cell{
					&models.Cell{Value: "John"},
					&models.Cell{Value: "Male"},
				},
			},
		}
		return nil
	}
	return errors.New("Invalid Query")
}

func (s *mockStore) GetPropertyLabels(ctx context.Context,
	in *pb.GetPropertyLabelsRequest, out *pb.GetPropertyLabelsResponse) error {
	return nil
}

func (s *mockStore) GetPropertyValues(ctx context.Context,
	in *pb.GetPropertyValuesRequest, out *pb.GetPropertyValuesResponse) error {
	return nil
}

func (s *mockStore) GetTriples(ctx context.Context,
	in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error {
	return nil
}

func (s *mockStore) GetPopObs(ctx context.Context,
	in *pb.GetPopObsRequest, out *pb.GetPopObsResponse) error {
	return nil
}

func (s *mockStore) GetPlaceObs(ctx context.Context,
	in *pb.GetPlaceObsRequest, out *pb.GetPlaceObsResponse) error {
	return nil
}

func (s *mockStore) GetPopulations(ctx context.Context,
	in *pb.GetPopulationsRequest, out *pb.GetPopulationsResponse) error {
	return nil
}

func (s *mockStore) GetObservations(ctx context.Context,
	in *pb.GetObservationsRequest, out *pb.GetObservationsResponse) error {
	return nil
}

func (s *mockStore) GetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error {
	return nil
}

func (s *mockStore) GetPlaceKML(ctx context.Context,
	in *pb.GetPlaceKMLRequest, out *pb.GetPlaceKMLResponse) error {
	return nil
}

func TestGet(t *testing.T) {

	for _, c := range []struct {
		url    string
		status int
		want   string
	}{
		{
			"/query?sparql=SELECT*FROM",
			http.StatusOK,
			`{"header":["name","gender"],"rows":[{"cells":[{"value":"John"},{"value":"Male"}]}]}`,
		},
		{
			"/query?dcid=badrequest",
			http.StatusBadRequest,
			`{"code":400,"message":"Request url has invalid parameters: schema: invalid path \"dcid\""}`,
		},
	} {
		req, err := http.NewRequest("GET", c.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler := AppHandler{QueryGetHandler, &mockStore{}}
		handler.ServeHTTP(rr, req)
		if status := rr.Code; status != c.status {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, c.status)
			continue
		}
		if diff := deep.Equal(c.want, strings.TrimSuffix(rr.Body.String(), "\n")); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}
