// Copyright 2026 Google LLC
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

package agent

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestObservationsDate(t *testing.T) {
	t.Run("validateDateFormat", func(t *testing.T) {
		tests := []struct {
			date    string
			wantErr bool
		}{
			{"2020", false},
			{"2020-05", false},
			{"2020-05-15", false},
			{dateTypeLatest, true},
			{dateTypeAll, true},
			{dateTypeRange, true},
			{"2020-99", true},
			{"2020-05-99", true},
			{"invalid", true},
		}
		for _, tc := range tests {
			err := validateDateFormat(tc.date)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateDateFormat(%q) error = %v, wantErr = %v", tc.date, err, tc.wantErr)
			}
		}
	})

	t.Run("parseDateFilter", func(t *testing.T) {
		tests := []struct {
			desc      string
			date      string
			start     string
			end       string
			wantType  string
			wantStart time.Time
			wantEnd   time.Time
			wantErr   bool
		}{
			{
				desc:     "default latest",
				date:     "",
				wantType: dateTypeLatest,
			},
			{
				desc:     "all",
				date:     dateTypeAll,
				wantType: dateTypeAll,
			},
			{
				desc:     "all with start date ignores start and returns all",
				date:     dateTypeAll,
				start:    "2020",
				wantType: dateTypeAll,
				wantErr:  false,
			},
			{
				desc:      "specific year",
				date:      "2020",
				wantType:  dateTypeRange,
				wantStart: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				wantEnd:   time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
			},
			{
				desc:      "specific month",
				date:      "2020-05",
				wantType:  dateTypeRange,
				wantStart: time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC),
				wantEnd:   time.Date(2020, 5, 31, 0, 0, 0, 0, time.UTC),
			},
			{
				desc:      "range with start and end",
				date:      dateTypeRange,
				start:     "2020",
				end:       "2022-05-15",
				wantType:  dateTypeRange,
				wantStart: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				wantEnd:   time.Date(2022, 5, 15, 0, 0, 0, 0, time.UTC),
			},
			{
				desc:    "range missing start and end",
				date:    dateTypeRange,
				wantErr: true,
			},
			{
				desc:    "range start after end",
				date:    dateTypeRange,
				start:   "2021",
				end:     "2020",
				wantErr: true,
			},
		}
		for _, tc := range tests {
			t.Run(tc.desc, func(t *testing.T) {
				filter, err := parseDateFilter(tc.date, tc.start, tc.end)
				if (err != nil) != tc.wantErr {
					t.Fatalf("parseDateFilter(%q, %q, %q) error = %v, wantErr = %v", tc.date, tc.start, tc.end, err, tc.wantErr)
				}
				if tc.wantErr {
					return
				}
				if filter.dateType != tc.wantType {
					t.Errorf("dateType = %q, want: %q", filter.dateType, tc.wantType)
				}
				if !filter.startDate.Equal(tc.wantStart) {
					t.Errorf("startDate = %v, want: %v", filter.startDate, tc.wantStart)
				}
				if !filter.endDate.Equal(tc.wantEnd) {
					t.Errorf("endDate = %v, want: %v", filter.endDate, tc.wantEnd)
				}
			})
		}
	})

	t.Run("isDateInInterval", func(t *testing.T) {
		filter2020, _ := parseDateFilter("2020", "", "")
		filterRange, _ := parseDateFilter(dateTypeRange, "2020-05", "2020-08-15")

		tests := []struct {
			desc    string
			obsDate string
			filter  *dateFilter
			want    bool
		}{
			{"latest always matches", "2019", &dateFilter{dateType: dateTypeLatest}, true},
			{"all always matches", "2019", &dateFilter{dateType: dateTypeAll}, true},
			{"exact year match", "2020-05-15", filter2020, true},
			{"exact year mismatch", "2021-01-01", filter2020, false},
			{"range match start boundary", "2020-05-01", filterRange, true},
			{"range match end boundary", "2020-08-15", filterRange, true},
			{"range match inside", "2020-06", filterRange, true},
			{"range mismatch before", "2020-04-30", filterRange, false},
			{"range mismatch after", "2020-08-16", filterRange, false},
			{"invalid obs date ignored", "invalid", filterRange, false},
		}
		for _, tc := range tests {
			t.Run(tc.desc, func(t *testing.T) {
				got := isDateInInterval(tc.obsDate, tc.filter)
				if got != tc.want {
					t.Errorf("isDateInInterval(%q) = %v, want: %v", tc.obsDate, got, tc.want)
				}
			})
		}
	})
}

type obsMockMixer struct {
	Mixer
	v2NodeFn   func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error)
	sdmxDataFn func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error)
}

func (m *obsMockMixer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	if m.v2NodeFn != nil {
		return m.v2NodeFn(ctx, in)
	}
	return nil, nil
}

func (m *obsMockMixer) SdmxData(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	if m.sdmxDataFn != nil {
		return m.sdmxDataFn(ctx, in)
	}
	return nil, nil
}

func TestGetObservations(t *testing.T) {
	t.Run("Validation Failures", func(t *testing.T) {
		mock := &obsMockMixer{}
		svc := NewService(mock, nil, nil)

		tests := []struct {
			desc    string
			req     *GetObservationsRequest
			wantErr codes.Code
		}{
			{
				desc:    "nil request",
				req:     nil,
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "missing variable",
				req: &GetObservationsRequest{
					Entities: map[string]*EntityConstraint{
						"observationAbout": {Dcids: []string{"geoId/06"}},
					},
				},
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "missing entities",
				req: &GetObservationsRequest{
					VariableDcid: "Count_Person",
				},
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "invalid date format",
				req: &GetObservationsRequest{
					VariableDcid: "Count_Person",
					Entities: map[string]*EntityConstraint{
						"observationAbout": {Dcids: []string{"geoId/06"}},
					},
					Date: "invalid",
				},
				wantErr: codes.InvalidArgument,
			},
		}

		for _, tc := range tests {
			t.Run(tc.desc, func(t *testing.T) {
				_, err := svc.GetObservations(context.Background(), tc.req)
				if err == nil {
					t.Fatalf("GetObservations succeeded, want error code: %v", tc.wantErr)
				}
				if status.Code(err) != tc.wantErr {
					t.Errorf("GetObservations returned error code: %v, want: %v", status.Code(err), tc.wantErr)
				}
			})
		}
	})

	t.Run("Multi-entity SDMX Query with Dual Tables", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Attributes: map[string]string{
								"facetId": "178129038",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2020", ObservationValue: "1671329"},
								{TimePeriod: "2021", ObservationValue: "1682353"},
							},
						},
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06003",
							},
							Attributes: map[string]string{
								"facetId": "178129038",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2020", ObservationValue: "1204"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
						"geoId/06003": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alpine County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001", "geoId/06003"}},
			},
		})
		if err != nil {
			t.Fatalf("GetObservations SDMX path failed: %v", err)
		}

		want := &GetObservationsResponse{
			Variable: &ObservationNode{
				Dcid: "Count_Person",
			},
			EntityMetadata: &Table{
				Columns: []string{"dcid", "name"},
				Rows: [][]any{
					{"geoId/06001", "Alameda County"},
					{"geoId/06003", "Alpine County"},
				},
			},
			Data: &Table{
				Columns: []string{"observationAbout", "date", "value"},
				Rows: [][]any{
					{"geoId/06001", "2021", float64(1682353)},
					{"geoId/06003", "2020", float64(1204)},
				},
			},
			SourceMetadata: &ObservationFacetMetadata{
				SourceID: "178129038",
			},
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("GetObservations SDMX response mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Parent Place Expansion (ContainedInPlace)", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				c, ok := in.GetConstraints()["observationAbout"]
				if !ok {
					t.Fatalf("missing constraint for observationAbout")
				}
				containedIn, ok := c.GetPropertyConstraints()["containedInPlace"]
				if !ok || !containedIn.GetTransitive() || containedIn.GetPredicates()[0].GetValue() != "geoId/06" {
					t.Errorf("invalid containedInPlace property constraint: %+v", containedIn)
				}
				typeOf, ok := c.GetPropertyConstraints()["typeOf"]
				if !ok || typeOf.GetPredicates()[0].GetValue() != "County" {
					t.Errorf("invalid typeOf property constraint: %+v", typeOf)
				}

				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Attributes: map[string]string{
								"facetId": "178129038",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2020", ObservationValue: "1671329"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {
					ParentSpec: &ParentPlaceSpec{
						ParentDcid: "geoId/06",
						ChildType:  "County",
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("GetObservations SDMX parent path failed: %v", err)
		}

		want := &GetObservationsResponse{
			Variable: &ObservationNode{
				Dcid: "Count_Person",
			},
			EntityMetadata: &Table{
				Columns: []string{"dcid", "name"},
				Rows: [][]any{
					{"geoId/06001", "Alameda County"},
				},
			},
			Data: &Table{
				Columns: []string{"observationAbout", "date", "value"},
				Rows: [][]any{
					{"geoId/06001", "2020", float64(1671329)},
				},
			},
			SourceMetadata: &ObservationFacetMetadata{
				SourceID: "178129038",
			},
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("GetObservations SDMX parent response mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Validation Failure: Multi-Parent Expansion Limit", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {ParentSpec: &ParentPlaceSpec{ParentDcid: "geoId/06", ChildType: "County"}},
				"comparisonPlace":  {ParentSpec: &ParentPlaceSpec{ParentDcid: "geoId/48", ChildType: "County"}},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for multi-parent expansion, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Empty String DCID in List", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06", ""}},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for empty string DCID, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Both Dcids and ParentSpec Specified", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {
					Dcids:      []string{"geoId/06"},
					ParentSpec: &ParentPlaceSpec{ParentDcid: "geoId/06", ChildType: "County"},
				},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error when both dcids and parent_spec are set, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Empty ParentSpec Fields", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {
					ParentSpec: &ParentPlaceSpec{ParentDcid: "", ChildType: "County"},
				},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for empty parent spec fields, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Empty Entity List", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for empty entity list, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Empty Slot Name", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"": {Dcids: []string{"geoId/06"}},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for empty entity slot name, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Reserved Slot Name", func(t *testing.T) {
		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"variableMeasured": {Dcids: []string{"Median_Age_Person"}},
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error for reserved entity slot name, got code: %v (err: %v)", status.Code(err), err)
		}
	})

	t.Run("Validation Failure: Context Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		svc := NewService(&obsMockMixer{}, nil, nil)
		_, err := svc.GetObservations(ctx, &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06"}},
			},
		})
		if err == nil {
			t.Errorf("expected error on canceled context, got nil")
		}
	})

	t.Run("SDMX Path Date Filtering (Specific Year)", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2019", ObservationValue: "10"},
								{TimePeriod: "2020", ObservationValue: "12"},
								{TimePeriod: "2021", ObservationValue: "14"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001"}},
			},
			Date: "2020",
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		wantRows := [][]any{
			{"geoId/06001", "2020", float64(12)},
		}

		if diff := cmp.Diff(wantRows, got.Data.Rows); diff != "" {
			t.Errorf("SDMX Date Filter rows mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("SDMX Path Date Filtering (All)", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2019", ObservationValue: "10"},
								{TimePeriod: "2020", ObservationValue: "12"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001"}},
			},
			Date: "all",
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		wantRows := [][]any{
			{"geoId/06001", "2019", float64(10)},
			{"geoId/06001", "2020", float64(12)},
		}

		if diff := cmp.Diff(wantRows, got.Data.Rows); diff != "" {
			t.Errorf("SDMX 'all' Date Filter rows mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("SDMX Path Date Filtering (Range)", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2018", ObservationValue: "8"},
								{TimePeriod: "2019", ObservationValue: "10"},
								{TimePeriod: "2020", ObservationValue: "12"},
								{TimePeriod: "2021", ObservationValue: "14"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001"}},
			},
			Date:           "range",
			DateRangeStart: "2019",
			DateRangeEnd:   "2020",
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		wantRows := [][]any{
			{"geoId/06001", "2019", float64(10)},
			{"geoId/06001", "2020", float64(12)},
		}

		if diff := cmp.Diff(wantRows, got.Data.Rows); diff != "" {
			t.Errorf("SDMX 'range' Date Filter rows mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("SDMX Path Date Filtering (Latest)", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2019-12", ObservationValue: "10"},
								{TimePeriod: "2020-01", ObservationValue: "12"},
								{TimePeriod: "2018-06", ObservationValue: "8"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06001": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Alameda County"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001"}},
			},
			Date: "latest",
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		wantRows := [][]any{
			{"geoId/06001", "2020-01", float64(12)},
		}

		if diff := cmp.Diff(wantRows, got.Data.Rows); diff != "" {
			t.Errorf("SDMX 'latest' Date Filter rows mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("SDMX Path Metadata Enrichment Network Fallback", func(t *testing.T) {
		mock := &obsMockMixer{
			sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
				return &sdmxpb.SdmxDataResult{
					Series: []*sdmxpb.SdmxTimeSeries{
						{
							Dimensions: map[string]string{
								"variableMeasured": "Count_Person",
								"observationAbout": "geoId/06001",
							},
							Points: []*sdmxpb.SdmxDataPoint{
								{TimePeriod: "2020", ObservationValue: "12"},
							},
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return nil, status.Error(codes.Unavailable, "V2Node service temporarily unavailable")
			},
		}

		svc := NewService(mock, nil, nil)
		got, err := svc.GetObservations(context.Background(), &GetObservationsRequest{
			VariableDcid: "Count_Person",
			Entities: map[string]*EntityConstraint{
				"observationAbout": {Dcids: []string{"geoId/06001"}},
			},
			Date: "latest",
		})
		if err != nil {
			t.Fatalf("GetObservations failed on metadata fallback test: %v", err)
		}

		wantMeta := &Table{
			Columns: []string{"dcid", "name"},
			Rows:    [][]any{{"geoId/06001", ""}},
		}

		if diff := cmp.Diff(wantMeta, got.EntityMetadata); diff != "" {
			t.Errorf("SDMX network fallback metadata mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestGetObservationsSdmxFacetSelection(t *testing.T) {
	nodeNames := map[string]string{
		"Count_Person": "Total Population",
		"geoId/06001":  "Alameda County",
		"geoId/06003":  "Alpine County",
		"geoId/06005":  "Amador County",
	}

	tests := []struct {
		desc           string
		entities       map[string][]string
		date           string
		dateRangeStart string
		dateRangeEnd   string
		series         []*sdmxpb.SdmxTimeSeries
		want           *GetObservationsResponse
	}{
		{
			desc:     "entity metadata only includes entities in data rows",
			entities: map[string][]string{"observationAbout": {"geoId/06001", "geoId/06003", "geoId/06005"}},
			date:     "2020",
			series: []*sdmxpb.SdmxTimeSeries{
				newTestSeries(map[string]string{"observationAbout": "geoId/06001"}, "f1", "2020"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06003"}, "f1", "2019"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06005"}, "f2", "2019"),
			},
			want: &GetObservationsResponse{
				Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"geoId/06001", "Alameda County"}},
				},
				Data: &Table{
					Columns: []string{"observationAbout", "date", "value"},
					Rows:    [][]any{{"geoId/06001", "2020", float64(1)}},
				},
				SourceMetadata: &ObservationFacetMetadata{SourceID: "f1"},
				AlternativeSources: []*AlternativeSource{
					{SourceMetadata: &ObservationFacetMetadata{SourceID: "f2"}},
				},
			},
		},
		{
			// Two places are requested, but the primary facet only covers one of them, so alternative
			// place counts are omitted (legacy gates on the primary source's place count).
			desc:     "alternative place counts are gated on the primary facet's place count",
			entities: map[string][]string{"observationAbout": {"geoId/06001", "geoId/06003"}},
			date:     "2020",
			series: []*sdmxpb.SdmxTimeSeries{
				newTestSeries(map[string]string{"observationAbout": "geoId/06001"}, "f1", "2020"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06003"}, "f2", "2020"),
			},
			want: &GetObservationsResponse{
				Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"geoId/06001", "Alameda County"}},
				},
				Data: &Table{
					Columns: []string{"observationAbout", "date", "value"},
					Rows:    [][]any{{"geoId/06001", "2020", float64(1)}},
				},
				SourceMetadata: &ObservationFacetMetadata{SourceID: "f1"},
				AlternativeSources: []*AlternativeSource{
					{SourceMetadata: &ObservationFacetMetadata{SourceID: "f2"}},
				},
			},
		},
		{
			desc:     "no data in the date window returns no entity metadata rows",
			entities: map[string][]string{"observationAbout": {"geoId/06001"}},
			date:     "2020",
			series: []*sdmxpb.SdmxTimeSeries{
				newTestSeries(map[string]string{"observationAbout": "geoId/06001"}, "f1", "2019"),
			},
			want: &GetObservationsResponse{
				Variable:       &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Data:           &Table{Columns: []string{"observationAbout", "date", "value"}},
				SourceMetadata: &ObservationFacetMetadata{SourceID: "f1"},
			},
		},
		{
			desc:           "places without data in the date window do not count toward a facet",
			entities:       map[string][]string{"observationAbout": {"geoId/06001", "geoId/06003"}},
			date:           "range",
			dateRangeStart: "2019",
			dateRangeEnd:   "2020",
			series: []*sdmxpb.SdmxTimeSeries{
				newTestSeries(map[string]string{"observationAbout": "geoId/06001"}, "f1", "2019", "2020"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06003"}, "f1", "2010"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06001"}, "f2", "2020"),
				newTestSeries(map[string]string{"observationAbout": "geoId/06003"}, "f2", "2019"),
			},
			want: &GetObservationsResponse{
				Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"geoId/06001", "Alameda County"},
						{"geoId/06003", "Alpine County"},
					},
				},
				Data: &Table{
					Columns: []string{"observationAbout", "date", "value"},
					Rows: [][]any{
						{"geoId/06001", "2020", float64(1)},
						{"geoId/06003", "2019", float64(1)},
					},
				},
				SourceMetadata: &ObservationFacetMetadata{SourceID: "f2"},
				AlternativeSources: []*AlternativeSource{
					{
						SourceMetadata:   &ObservationFacetMetadata{SourceID: "f1"},
						PlacesFoundCount: proto.Int32(1),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			mock := &obsMockMixer{
				sdmxDataFn: func(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
					return &sdmxpb.SdmxDataResult{Series: test.series}, nil
				},
				v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
					return newTestNodeResponse(in.GetNodes(), nodeNames), nil
				},
			}

			entities := make(map[string]*EntityConstraint, len(test.entities))
			for slot, dcids := range test.entities {
				entities[slot] = &EntityConstraint{Dcids: dcids}
			}
			req := &GetObservationsRequest{
				VariableDcid:   "Count_Person",
				Entities:       entities,
				Date:           test.date,
				DateRangeStart: test.dateRangeStart,
				DateRangeEnd:   test.dateRangeEnd,
			}

			got, err := NewService(mock, nil, nil).GetObservations(context.Background(), req)
			if err != nil {
				t.Fatalf("GetObservations() unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("GetObservations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// newTestSeries builds a Count_Person series with the given dimensions and a value of "1" at each date.
func newTestSeries(dims map[string]string, facetID string, dates ...string) *sdmxpb.SdmxTimeSeries {
	series := &sdmxpb.SdmxTimeSeries{
		Dimensions: map[string]string{"variableMeasured": "Count_Person"},
		Attributes: map[string]string{"facetId": facetID},
	}
	for k, v := range dims {
		series.Dimensions[k] = v
	}
	for _, d := range dates {
		series.Points = append(series.Points, &sdmxpb.SdmxDataPoint{TimePeriod: d, ObservationValue: "1"})
	}
	return series
}

// newTestNodeResponse returns name arcs for the requested nodes found in names.
func newTestNodeResponse(nodes []string, names map[string]string) *pbv2.NodeResponse {
	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, dcid := range nodes {
		arcs := make(map[string]*pbv2.Nodes)
		if name, ok := names[dcid]; ok {
			arcs[arcName] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Value: name}}}
		}
		resp.Data[dcid] = &pbv2.LinkedGraph{Arcs: arcs}
	}
	return resp
}

// truncatingNodeMixer fakes the way V2Node truncates. It sorts the requested nodes,
// fills in only the ones that fit on the page, and returns an empty LinkedGraph for
// the rest plus a NextToken. So a caller that ignores the token sees blank names,
// not missing entries.
type truncatingNodeMixer struct {
	Mixer

	nodesPerPage int

	mu    sync.Mutex
	calls int
}

func (m *truncatingNodeMixer) V2Node(_ context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()

	sorted := slices.Clone(req.GetNodes())
	sort.Strings(sorted)

	start := 0
	if token := req.GetNextToken(); token != "" {
		if _, err := fmt.Sscanf(token, "offset:%d", &start); err != nil {
			return nil, fmt.Errorf("malformed test token %q: %w", token, err)
		}
	}
	end := min(start+m.nodesPerPage, len(sorted))

	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for i, dcid := range sorted {
		if i < start || i >= end {
			resp.Data[dcid] = &pbv2.LinkedGraph{}
			continue
		}
		resp.Data[dcid] = &pbv2.LinkedGraph{
			Arcs: map[string]*pbv2.Nodes{
				arcName: {Nodes: []*pb.EntityInfo{{Value: dcid + " Name"}}},
			},
		}
	}
	if end < len(sorted) {
		resp.NextToken = fmt.Sprintf("offset:%d", end)
	}
	return resp, nil
}

func TestFetchEntityPropertiesPagination(t *testing.T) {
	tests := []struct {
		name         string
		entityCount  int
		nodesPerPage int
	}{
		{
			name:         "SinglePageNeedsNoPagination",
			entityCount:  10,
			nodesPerPage: 150,
		},
		{
			name:         "EntitiesPastTheFirstPageAreStillResolved",
			entityCount:  300,
			nodesPerPage: 150,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dcids := make([]string, 0, test.entityCount)
			want := make(map[string]*nodeProperties, test.entityCount)
			for i := range test.entityCount {
				dcid := fmt.Sprintf("geoId/%05d", i)
				dcids = append(dcids, dcid)
				want[dcid] = &nodeProperties{name: dcid + " Name"}
			}

			svc := NewService(&truncatingNodeMixer{nodesPerPage: test.nodesPerPage}, nil, nil)

			got, err := svc.fetchEntityProperties(context.Background(), dcids)
			if err != nil {
				t.Fatalf("fetchEntityProperties() unexpected error: %v", err)
			}

			if diff := cmp.Diff(want, got, cmp.AllowUnexported(nodeProperties{})); diff != "" {
				t.Errorf("fetchEntityProperties() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
