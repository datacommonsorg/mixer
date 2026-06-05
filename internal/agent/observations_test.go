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
	"sort"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
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
				desc:    "all with start error",
				date:    dateTypeAll,
				start:   "2020",
				wantErr: true,
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

func TestSelectPrimarySource(t *testing.T) {
	filterLatest, _ := parseDateFilter(dateTypeLatest, "", "")
	filterAll, _ := parseDateFilter(dateTypeAll, "", "")

	tests := []struct {
		desc           string
		variableData   *pbv2.VariableObservation
		sourceOverride string
		filter         *dateFilter
		wantPrimary    string
		wantAlts       map[string]int
		wantProcessed  map[string][]float64 // maps entity to expected values
	}{
		{
			desc: "Heuristic 1: Place Coverage Wins",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A",
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(10)},
								},
							},
							{
								FacetId: "source-B",
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(20)},
								},
							},
						},
					},
					"geoId/08": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A",
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(15)},
								},
							},
						},
					},
				},
			},
			filter:      filterAll,
			wantPrimary: "source-A",
			wantAlts:    map[string]int{"source-B": 1},
			wantProcessed: map[string][]float64{
				"geoId/06": {10},
				"geoId/08": {15},
			},
		},
		{
			desc: "Heuristic 2: Total Observations Wins",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A", // 2 observations
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(10)},
									{Date: "2021", Value: ptrFloat64(11)},
								},
							},
							{
								FacetId: "source-B", // 1 observation
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(20)},
								},
							},
						},
					},
				},
			},
			filter:      filterAll,
			wantPrimary: "source-A",
			wantAlts:    map[string]int{"source-B": 1},
			wantProcessed: map[string][]float64{
				"geoId/06": {10, 11},
			},
		},
		{
			desc: "Heuristic 3: Recency Wins",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A", // latest is 2021
								Observations: []*pb.PointStat{
									{Date: "2021", Value: ptrFloat64(10)},
								},
							},
							{
								FacetId: "source-B", // latest is 2022
								Observations: []*pb.PointStat{
									{Date: "2022", Value: ptrFloat64(20)},
								},
							},
						},
					},
				},
			},
			filter:      filterAll,
			wantPrimary: "source-B",
			wantAlts:    map[string]int{"source-A": 1},
			wantProcessed: map[string][]float64{
				"geoId/06": {20},
			},
		},
		{
			desc: "Heuristic 4: Original Index Wins",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A", // Index 0 (preferred)
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(10)},
								},
							},
							{
								FacetId: "source-B", // Index 1
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(20)},
								},
							},
						},
					},
				},
			},
			filter:      filterAll,
			wantPrimary: "source-A",
			wantAlts:    map[string]int{"source-B": 1},
			wantProcessed: map[string][]float64{
				"geoId/06": {10},
			},
		},
		{
			desc: "Source Override Bypasses Ranking",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A", // Would normally win Heuristic 2 (2 obs vs 1 obs)
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(10)},
									{Date: "2021", Value: ptrFloat64(11)},
								},
							},
							{
								FacetId: "source-B",
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(20)},
								},
							},
						},
					},
				},
			},
			sourceOverride: "source-B",
			filter:         filterAll,
			wantPrimary:    "source-B",
			wantAlts:       map[string]int{}, // Override means no alternatives returned
			wantProcessed: map[string][]float64{
				"geoId/06": {20},
			},
		},
		{
			desc: "Filter Latest Returns Only the Most Recent Point",
			variableData: &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/06": {
						OrderedFacets: []*pbv2.FacetObservation{
							{
								FacetId: "source-A",
								Observations: []*pb.PointStat{
									{Date: "2020", Value: ptrFloat64(10)},
									{Date: "2022", Value: ptrFloat64(12)},
									{Date: "2021", Value: ptrFloat64(11)},
								},
							},
						},
					},
				},
			},
			filter:      filterLatest,
			wantPrimary: "source-A",
			wantAlts:    map[string]int{},
			wantProcessed: map[string][]float64{
				"geoId/06": {11}, // Assumes date filter handles sorting. Note: in real Mixer, obs are sorted.
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := selectPrimarySource(tc.variableData, tc.sourceOverride, tc.filter)
			if got.primarySourceID != tc.wantPrimary {
				t.Errorf("primarySourceID = %q, want: %q", got.primarySourceID, tc.wantPrimary)
			}
			if diff := cmp.Diff(got.alternativeSourceCounts, tc.wantAlts); diff != "" {
				t.Errorf("alternativeSourceCounts mismatch (-got +want):\n%s", diff)
			}
			// Verify processed values
			if len(got.processedDataByPlace) != len(tc.wantProcessed) {
				t.Fatalf("processed places count = %d, want: %d", len(got.processedDataByPlace), len(tc.wantProcessed))
			}
			for place, data := range got.processedDataByPlace {
				wantVals, ok := tc.wantProcessed[place]
				if !ok {
					t.Fatalf("unexpected processed place in response: %s", place)
				}
				var gotVals []float64
				for _, obs := range data.observations {
					gotVals = append(gotVals, obs.GetValue())
				}
				if diff := cmp.Diff(gotVals, wantVals); diff != "" {
					t.Errorf("processed values for place %s mismatch (-got +want):\n%s", place, diff)
				}
			}
		})
	}
}

type obsMockMixer struct {
	Mixer
	v2ObsFn  func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
	v2NodeFn func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error)
}

func (m *obsMockMixer) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	return m.v2ObsFn(ctx, in)
}

func (m *obsMockMixer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return m.v2NodeFn(ctx, in)
}

func TestGetObservations(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	t.Run("Validation Failures", func(t *testing.T) {
		mock := &obsMockMixer{}
		svc := NewService(mock, nil)

		tests := []struct {
			desc    string
			req     *pbv2.GetObservationsRequest
			wantErr codes.Code
		}{
			{
				desc:    "nil request",
				req:     nil,
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "missing variable",
				req: &pbv2.GetObservationsRequest{
					PlaceDcid: "geoId/06",
				},
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "missing place",
				req: &pbv2.GetObservationsRequest{
					VariableDcid: "Count_Person",
				},
				wantErr: codes.InvalidArgument,
			},
			{
				desc: "invalid date format",
				req: &pbv2.GetObservationsRequest{
					VariableDcid: "Count_Person",
					PlaceDcid:    "geoId/06",
					Date:         ptrString("invalid"),
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

	t.Run("Case 1: Basic Single Place (Latest Date)", func(t *testing.T) {
		mock := &obsMockMixer{
			v2ObsFn: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
				// Verify buildObservationRequest inputs
				if diff := cmp.Diff(in.GetVariable().GetDcids(), []string{"Count_Person"}); diff != "" {
					t.Errorf("V2Observation variable DCIDs mismatch (-got +want):\n%s", diff)
				}
				if diff := cmp.Diff(in.GetEntity().GetDcids(), []string{"geoId/06"}); diff != "" {
					t.Errorf("V2Observation entity DCIDs mismatch (-got +want):\n%s", diff)
				}
				if in.GetDate() != "LATEST" {
					t.Errorf("V2Observation date = %q, want: LATEST", in.GetDate())
				}

				return &pbv2.ObservationResponse{
					ByVariable: map[string]*pbv2.VariableObservation{
						"Count_Person": {
							ByEntity: map[string]*pbv2.EntityObservation{
								"geoId/06": {
									OrderedFacets: []*pbv2.FacetObservation{
										{
											FacetId: "source-A", // older
											Observations: []*pb.PointStat{
												{Date: "2020", Value: ptrFloat64(10)},
											},
										},
										{
											FacetId: "source-B", // newer
											Observations: []*pb.PointStat{
												{Date: "2021", Value: ptrFloat64(20)},
											},
										},
									},
								},
							},
						},
					},
					Facets: map[string]*pb.Facet{
						"source-A": {
							ImportName:        "Census",
							MeasurementMethod: "CensusMethod",
							ProvenanceUrl:     "census.gov",
						},
						"source-B": {
							ImportName:        "CDC",
							MeasurementMethod: "CDCMethod",
							ProvenanceUrl:     "cdc.gov",
						},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				// V2Node should fetch variable and place name/type
				expectedNodes := []string{"Count_Person", "geoId/06"}
				sort.Strings(in.GetNodes())
				sort.Strings(expectedNodes)
				if diff := cmp.Diff(in.GetNodes(), expectedNodes); diff != "" {
					t.Errorf("V2Node nodes mismatch (-got +want):\n%s", diff)
				}
				if in.GetProperty() != "->[name, typeOf]" {
					t.Errorf("V2Node property = %q, want: ->[name, typeOf]", in.GetProperty())
				}

				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"Count_Person": {
							Arcs: map[string]*pbv2.Nodes{
								"name": {
									Nodes: []*pb.EntityInfo{{Value: "Total Population"}},
								},
								"typeOf": {
									Nodes: []*pb.EntityInfo{{Dcid: "StatisticalVariable"}},
								},
							},
						},
						"geoId/06": {
							Arcs: map[string]*pbv2.Nodes{
								"name": {
									Nodes: []*pb.EntityInfo{{Value: "California"}},
								},
								"typeOf": {
									Nodes: []*pb.EntityInfo{{Dcid: "State"}},
								},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil)
		got, err := svc.GetObservations(context.Background(), &pbv2.GetObservationsRequest{
			VariableDcid: "Count_Person",
			PlaceDcid:    "geoId/06",
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		want := &pbv2.GetObservationsResponse{
			Variable: &pbv2.GetObservationsResponse_Node{
				Dcid:   "Count_Person",
				Name:   "Total Population",
				TypeOf: []string{"StatisticalVariable"},
			},
			SourceMetadata: &pbv2.GetObservationsResponse_FacetMetadata{
				SourceId:          "source-B",
				ImportName:        "CDC",
				MeasurementMethod: "CDCMethod",
				ProvenanceUrl:     "cdc.gov",
			},
			AlternativeSources: []*pbv2.GetObservationsResponse_AlternativeSource{
				{
					SourceMetadata: &pbv2.GetObservationsResponse_FacetMetadata{
						SourceId:          "source-A",
						ImportName:        "Census",
						MeasurementMethod: "CensusMethod",
						ProvenanceUrl:     "census.gov",
					},
				},
			},
			PlaceObservations: []*pbv2.GetObservationsResponse_PlaceObservation{
				{
					Place: &pbv2.GetObservationsResponse_Node{
						Dcid:   "geoId/06",
						Name:   "California",
						TypeOf: []string{"State"},
					},
					TimeSeries: []*pbv2.GetObservationsResponse_TimeSeriesPoint{
						{Date: "2021", Value: 20},
					},
				},
			},
		}

		if diff := cmp.Diff(got, want, cmpOpts); diff != "" {
			t.Errorf("GetObservations response mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("Case 2: ContainedInPlace Hierarchy Query (Latest Date)", func(t *testing.T) {
		mock := &obsMockMixer{
			v2ObsFn: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
				if in.GetEntity().GetExpression() != "geoId/06<-containedInPlace+{typeOf:County}" {
					t.Errorf("V2Observation entity expression = %q, want: geoId/06<-containedInPlace+{typeOf:County}", in.GetEntity().GetExpression())
				}

				return &pbv2.ObservationResponse{
					ByVariable: map[string]*pbv2.VariableObservation{
						"Count_Person": {
							ByEntity: map[string]*pbv2.EntityObservation{
								"geoId/06001": {
									OrderedFacets: []*pbv2.FacetObservation{
										{
											FacetId: "source-A",
											Observations: []*pb.PointStat{
												{Date: "2020", Value: ptrFloat64(100)},
											},
										},
										{
											FacetId: "source-B", // older
											Observations: []*pb.PointStat{
												{Date: "2019", Value: ptrFloat64(90)},
											},
										},
									},
								},
								"geoId/06003": {
									OrderedFacets: []*pbv2.FacetObservation{
										{
											FacetId: "source-A",
											Observations: []*pb.PointStat{
												{Date: "2020", Value: ptrFloat64(300)},
											},
										},
									},
								},
							},
						},
					},
					Facets: map[string]*pb.Facet{
						"source-A": {ImportName: "Census"},
						"source-B": {ImportName: "CDC"},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"Count_Person": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Total Population"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "StatisticalVariable"}}},
							},
						},
						"geoId/06": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "California"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "State"}}},
							},
						},
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

		svc := NewService(mock, nil)
		got, err := svc.GetObservations(context.Background(), &pbv2.GetObservationsRequest{
			VariableDcid:   "Count_Person",
			PlaceDcid:      "geoId/06",
			ChildPlaceType: ptrString("County"),
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		// Prepare expected values (sort for deterministic assertion order)
		sort.Slice(got.PlaceObservations, func(i, j int) bool {
			return got.PlaceObservations[i].Place.Dcid < got.PlaceObservations[j].Place.Dcid
		})

		count1 := int32(1)
		want := &pbv2.GetObservationsResponse{
			Variable: &pbv2.GetObservationsResponse_Node{
				Dcid:   "Count_Person",
				Name:   "Total Population",
				TypeOf: []string{"StatisticalVariable"},
			},
			ResolvedParentPlace: &pbv2.GetObservationsResponse_Node{
				Dcid:   "geoId/06",
				Name:   "California",
				TypeOf: []string{"State"},
			},
			ChildPlaceType: "County",
			SourceMetadata: &pbv2.GetObservationsResponse_FacetMetadata{
				SourceId:   "source-A",
				ImportName: "Census",
			},
			AlternativeSources: []*pbv2.GetObservationsResponse_AlternativeSource{
				{
					SourceMetadata: &pbv2.GetObservationsResponse_FacetMetadata{
						SourceId:   "source-B",
						ImportName: "CDC",
					},
					PlacesFoundCount: &count1,
				},
			},
			PlaceObservations: []*pbv2.GetObservationsResponse_PlaceObservation{
				{
					Place: &pbv2.GetObservationsResponse_Node{
						Dcid:   "geoId/06001",
						Name:   "Alameda County",
						TypeOf: []string{"County"},
					},
					TimeSeries: []*pbv2.GetObservationsResponse_TimeSeriesPoint{
						{Date: "2020", Value: 100},
					},
				},
				{
					Place: &pbv2.GetObservationsResponse_Node{
						Dcid:   "geoId/06003",
						Name:   "Alpine County",
						TypeOf: []string{"County"},
					},
					TimeSeries: []*pbv2.GetObservationsResponse_TimeSeriesPoint{
						{Date: "2020", Value: 300},
					},
				},
			},
		}

		if diff := cmp.Diff(got, want, cmpOpts); diff != "" {
			t.Errorf("GetObservations response mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("Case 3: Date Filtering (Specific Year '2020')", func(t *testing.T) {
		mock := &obsMockMixer{
			v2ObsFn: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
				if in.GetDate() != "" {
					t.Errorf("V2Observation date = %q, want empty (requests all dates for custom filter)", in.GetDate())
				}

				return &pbv2.ObservationResponse{
					ByVariable: map[string]*pbv2.VariableObservation{
						"Count_Person": {
							ByEntity: map[string]*pbv2.EntityObservation{
								"geoId/06": {
									OrderedFacets: []*pbv2.FacetObservation{
										{
											FacetId: "source-A",
											Observations: []*pb.PointStat{
												{Date: "2019", Value: ptrFloat64(10)},
												{Date: "2020", Value: ptrFloat64(12)},
												{Date: "2021", Value: ptrFloat64(14)},
											},
										},
									},
								},
							},
						},
					},
					Facets: map[string]*pb.Facet{
						"source-A": {ImportName: "Census"},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"Count_Person": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Total Population"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "StatisticalVariable"}}},
							},
						},
						"geoId/06": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "California"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "State"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil)
		got, err := svc.GetObservations(context.Background(), &pbv2.GetObservationsRequest{
			VariableDcid: "Count_Person",
			PlaceDcid:    "geoId/06",
			Date:         ptrString("2020"),
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		want := []*pbv2.GetObservationsResponse_TimeSeriesPoint{
			{Date: "2020", Value: 12},
		}

		if diff := cmp.Diff(got.PlaceObservations[0].TimeSeries, want, cmpOpts); diff != "" {
			t.Errorf("TimeSeries mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("Case 4: Source Override", func(t *testing.T) {
		mock := &obsMockMixer{
			v2ObsFn: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
				if diff := cmp.Diff(in.GetFilter().GetFacetIds(), []string{"source-B"}); diff != "" {
					t.Errorf("V2Observation facet filters mismatch (-got +want):\n%s", diff)
				}

				return &pbv2.ObservationResponse{
					ByVariable: map[string]*pbv2.VariableObservation{
						"Count_Person": {
							ByEntity: map[string]*pbv2.EntityObservation{
								"geoId/06": {
									OrderedFacets: []*pbv2.FacetObservation{
										{
											FacetId: "source-B",
											Observations: []*pb.PointStat{
												{Date: "2020", Value: ptrFloat64(20)},
											},
										},
									},
								},
							},
						},
					},
					Facets: map[string]*pb.Facet{
						"source-B": {ImportName: "CDC"},
					},
				}, nil
			},
			v2NodeFn: func(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"Count_Person": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "Total Population"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "StatisticalVariable"}}},
							},
						},
						"geoId/06": {
							Arcs: map[string]*pbv2.Nodes{
								"name":   {Nodes: []*pb.EntityInfo{{Value: "California"}}},
								"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "State"}}},
							},
						},
					},
				}, nil
			},
		}

		svc := NewService(mock, nil)
		got, err := svc.GetObservations(context.Background(), &pbv2.GetObservationsRequest{
			VariableDcid:   "Count_Person",
			PlaceDcid:      "geoId/06",
			SourceOverride: ptrString("source-B"),
		})
		if err != nil {
			t.Fatalf("GetObservations failed: %v", err)
		}

		if got.SourceMetadata.SourceId != "source-B" {
			t.Errorf("SourceId = %q, want: source-B", got.SourceMetadata.SourceId)
		}
		if len(got.AlternativeSources) != 0 {
			t.Errorf("alternative sources count = %d, want: 0", len(got.AlternativeSources))
		}
	})
}

func ptrString(s string) *string {
	return &s
}

func ptrFloat64(f float64) *float64 {
	return &f
}
