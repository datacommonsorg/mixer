// Copyright 2022 Google LLC
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

package util

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	v1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestZipAndEndocde(t *testing.T) {
	for _, c := range [][]byte{
		[]byte("abc123"),
		[]byte("<a>abc</a>"),
		[]byte("[\"a\":{\"b\":\"c\"}]"),
	} {
		r1, err := ZipAndEncode(c)
		if err != nil {
			t.Errorf("ZipAndEncode(%v) = %v", c, err)
			continue
		}

		r2, err := UnzipAndDecode(r1)
		if err != nil {
			t.Errorf("UnzipAndDecode(%v) = %v", r1, err)
			continue
		}

		if got, want := r2, c; string(got) != string(want) {
			t.Errorf("UnzipAndDecode(ZipAndEncode()) = %v, want %v", got, want)
		}
	}
}

func TestSnakeToCamel(t *testing.T) {
	for _, c := range []struct {
		input string
		want  string
	}{
		{"abc_def_g", "abcDefG"},
		{"abcDefG", "abcDefG"},
		{"_abc_d", "abcD"},
		{"abc_d_", "abcD"},
	} {
		if got := SnakeToCamel(c.input); got != c.want {
			t.Errorf("SnakeToCamel(%v) = %v, want %v", c.input, got, c.want)
		}
	}
}

func TestCheckValidDCIDs(t *testing.T) {
	for _, c := range []struct {
		dcids []string
		valid bool
	}{
		{[]string{"abc", "geoId/12"}, true},
		{[]string{"a bc"}, false},
		{[]string{"abc "}, false},
		{[]string{"abc,efd"}, false},
	} {
		if err := CheckValidDCIDs(c.dcids); (err == nil) != c.valid {
			t.Errorf("CheckValidDCIDs(%v) = %v, want %v", c.dcids, err, c.valid)
		}
	}
}

func TestMergeDedupe(t *testing.T) {
	for _, c := range []struct {
		strLists [][]string
		want     []string
	}{
		{[][]string{{"abc", "geoId/12"}, {"abc"}}, []string{"abc", "geoId/12"}},
		{[][]string{{"a", "bc"}, {"a", "bc", "d"}, {"f"}}, []string{"a", "bc", "d", "f"}},
		{[][]string{{"abc"}, {"ef"}}, []string{"abc", "ef"}},
		{[][]string{{"a", "a"}, {"b"}}, []string{"a", "b"}},
	} {
		got := MergeDedupe(c.strLists...)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("MergeDedupe got diff %+v", diff)
		}
	}
}

func TestSample(t *testing.T) {
	for _, c := range []struct {
		input    protoreflect.ProtoMessage
		expected protoreflect.ProtoMessage
		strategy *SamplingStrategy
	}{
		{
			&v1.PlacePageResponse{
				ChildPlacesType: "Country",
				ChildPlaces: []string{
					"geoId/12345",
					"geoId/54321",
				},
				StatVarSeries: map[string]*pb.StatVarSeries{
					"country/USA": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2011": 1010,
									"2012": 1020,
									"2013": 1030,
									"2014": 1040,
									"2015": 1050,
									"2016": 1060,
								},
							},
						},
					},
					"geoId/06": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2018": 300,
									"2019": 400,
									"2020": 500,
								},
							},
						},
					},
					"geoId/11": {
						Data: map[string]*pb.Series{
							"stat-var-2": {
								Val: map[string]float64{
									"2019": 350,
									"2020": 450,
								},
							},
						},
					},
				},
			},
			&v1.PlacePageResponse{
				ChildPlacesType: "Country",
				ChildPlaces: []string{
					"geoId/12345",
					"geoId/54321",
				},
				StatVarSeries: map[string]*pb.StatVarSeries{
					"country/USA": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2012": 1020,
									"2014": 1040,
									"2016": 1060,
								},
							},
						},
					},
					"geoId/06": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2018": 300,
									"2019": 400,
									"2020": 500,
								},
							},
						},
					},
					"geoId/11": {
						Data: map[string]*pb.Series{
							"stat-var-2": {
								Val: map[string]float64{
									"2019": 350,
									"2020": 450,
								},
							},
						},
					},
				},
			},
			&SamplingStrategy{
				Children: map[string]*SamplingStrategy{
					"statVarSeries": {
						MaxSample: -1,
						Children: map[string]*SamplingStrategy{
							"data": {
								MaxSample: -1,
								Children: map[string]*SamplingStrategy{
									"val": {
										MaxSample: 3,
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		got := Sample(c.input, c.strategy)
		if diff := cmp.Diff(got, c.expected, protocmp.Transform()); diff != "" {
			t.Errorf("Sample got diff %+v", diff)
		}
	}
}

func TestKeysToSlice(t *testing.T) {
	m := map[string]bool{
		"1": true,
		"2": true,
		"3": true,
	}
	expected := []string{"1", "2", "3"}
	result := KeysToSlice(m)
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("places.keysToSlice(%v) = %v; expected %v", m, result, expected)
	}
}

func TestEncode(t *testing.T) {
	for _, c := range []struct {
		info  *v1.PaginationInfo
		token string
	}{
		{
			// One entity scenario.
			&v1.PaginationInfo{
				CursorGroups: []*v1.CursorGroup{
					{
						Cursors: []*v1.Cursor{
							{
								ImportGroup: 0,
								Page:        0,
								Item:        20,
							},
							{
								ImportGroup: 1,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 2,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 3,
								Page:        2,
								Item:        50,
							},
							{
								ImportGroup: 4,
								Page:        1,
								Item:        10,
							},
						},
					},
				},
			},
			"H4sIAAAAAAAA/+JSEWKSEBFi42AUYJTgEmLjYILSzAJMEkZCbBwsID4AAAD//wEAAP//htWoVyYAAAA=",
		},
		{
			// Multiple entity scenario.
			&v1.PaginationInfo{
				CursorGroups: []*v1.CursorGroup{
					{
						Keys: []string{"geoId/05"},
						Cursors: []*v1.Cursor{
							{
								ImportGroup: 0,
								Page:        0,
								Item:        20,
							},
							{
								ImportGroup: 1,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 2,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 3,
								Page:        2,
								Item:        50,
							},
							{
								ImportGroup: 4,
								Page:        1,
								Item:        10,
							},
						},
					},
					{
						Keys: []string{"geoId/06"},
						Cursors: []*v1.Cursor{
							{
								ImportGroup: 0,
								Page:        5,
								Item:        200,
							},
							{
								ImportGroup: 1,
								Page:        8,
								Item:        100,
							},
							{
								ImportGroup: 2,
								Page:        7,
								Item:        150,
							},
							{
								ImportGroup: 3,
								Page:        15,
								Item:        60,
							},
							{
								ImportGroup: 4,
								Page:        4,
								Item:        40,
							},
						},
					},
				},
			},
			"H4sIAAAAAAAA/+LS4+JIT833TNE3MBVikhARYuNgFGCU4BJi42CC0swCTBJGQmwcLCA+lxFcvZkQqwCrxAlGsBYOiRQhdg4mAXaJaYxgPfwSNmA9LBIaAAAAAP//AQAA//82b3t4ZAAAAA==",
		},
	} {
		token, err := EncodeProto(c.info)
		if err != nil {
			t.Errorf("EncodeProto() got err %v", err)
			continue
		}
		if diff := cmp.Diff(token, c.token); diff != "" {
			t.Errorf("getScorePb() got diff score %v", diff)
		}
	}
}

func TestStringListIntersection(t *testing.T) {
	for _, c := range []struct {
		list [][]string
		want []string
	}{
		{
			[][]string{
				{"a", "b", "c"},
				{"a", "c", "d"},
				{"a", "c", "e", "f"},
			},
			[]string{"a", "c"},
		},
	} {
		got := StringListIntersection(c.list)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("StringListIntersection() = %v, want %v", got, c.want)
		}
	}
}

func TestGetFacetID(t *testing.T) {
	for _, c := range []struct {
		facet *pb.Facet
		want  string
	}{
		{
			&pb.Facet{
				ImportName: "test_import",
			},
			"2762939119",
		},
		{
			&pb.Facet{
				ImportName:    "test_import",
				IsDcAggregate: true,
			},
			"2985056674",
		},
	} {
		got := GetFacetID(c.facet)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("GetFacetID(%v) = %v, want %v", c.facet, got, c.want)
		}
	}
}

func TestShouldIncludeFacet(t *testing.T) {
	for _, c := range []struct {
		filter  *pbv2.FacetFilter
		facet   *pb.Facet
		facetId string
		want    bool
	}{
		{
			nil,
			&pb.Facet{},
			"",
			true,
		},
		{
			&pbv2.FacetFilter{
				Domains: []string{"cdc.gov"},
			},
			&pb.Facet{
				ProvenanceUrl: "https://wonder.cdc.gov/ucd-icd10.html",
			},
			"",
			true,
		},
		{
			&pbv2.FacetFilter{
				FacetIds: []string{"1", "2"},
			},
			&pb.Facet{
				ProvenanceUrl: "https://wonder.cdc.gov/ucd-icd10.html",
			},
			"",
			false,
		},
		{
			&pbv2.FacetFilter{
				FacetIds: []string{"1", "2"},
			},
			&pb.Facet{
				ProvenanceUrl: "https://wonder.cdc.gov/ucd-icd10.html",
			},
			"1",
			true,
		},
	} {
		got := ShouldIncludeFacet(c.filter, c.facet, c.facetId)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ShouldIncludeFacet(%v, %v) = %v, want %v", c.filter, c.facet, got, c.want)
		}
	}
}

func TestToStringListValue(t *testing.T) {
	for _, c := range []struct {
		desc  string
		input []string
		want  *structpb.ListValue
	}{
		{
			desc:  "non-empty list of strings",
			input: []string{"Place", "State"},
			want: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("Place"),
					structpb.NewStringValue("State"),
				},
			},
		},
		{
			desc:  "empty list of strings",
			input: []string{},
			want: &structpb.ListValue{
				Values: []*structpb.Value{},
			},
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			got := ToStringListValue(c.input)
			if diff := cmp.Diff(got, c.want, protocmp.Transform()); diff != "" {
				t.Errorf("ToStringListValue(%v) returned unexpected diff (-got +want):\n%s", c.input, diff)
			}
		})
	}
}

func TestSortedStringKeys(t *testing.T) {
	input := map[string]int{
		"recipient":          1,
		"donor":              2,
		"observationAbout":   3,
	}
	want := []string{"donor", "observationAbout", "recipient"}
	got := SortedStringKeys(input)
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("SortedStringKeys mismatch (-got +want):\n%s", diff)
	}
}

func TestFetchRemote(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	largeErrorBody := strings.Repeat("A", 10000)

	tests := []struct {
		name              string
		ctx               context.Context
		statusCode        int
		responseBody      string
		wantErr           bool
		errContains       string
		checkHeader       bool
		wantSurfaceHeader string
		wantRemoteHeader  string
		wantNodeName      string
	}{
		{
			name:         "success 200",
			ctx:          context.Background(),
			statusCode:   http.StatusOK,
			responseBody: `{"data":{"geoId/06":{"arcs":{"name":{"nodes":[{"name":"California"}]}}}}}`,
			wantErr:      false,
			wantNodeName: "California",
		},
		{
			name:         "error 400 with json body",
			ctx:          context.Background(),
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error":"invalid argument: place not found"}`,
			wantErr:      true,
			errContains:  "remote mixer response not ok: 400 Bad Request, body: {\"error\":\"invalid argument: place not found\"}",
		},
		{
			name:         "error 500 with truncated body",
			ctx:          context.Background(),
			statusCode:   http.StatusInternalServerError,
			responseBody: largeErrorBody,
			wantErr:      true,
			errContains:  "... (truncated)",
		},
		{
			name:        "context canceled",
			ctx:         cancelledCtx,
			statusCode:  http.StatusOK,
			wantErr:     true,
			errContains: "context canceled",
		},
		{
			name:              "surface header forwarded from context",
			ctx:               metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-surface", "mcp-server")),
			statusCode:        http.StatusOK,
			responseBody:      `{}`,
			wantErr:           false,
			checkHeader:       true,
			wantSurfaceHeader: "mcp-server",
			wantRemoteHeader:  "true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotSurfaceHeader, gotRemoteHeader string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotSurfaceHeader = r.Header.Get("X-Surface")
				gotRemoteHeader = r.Header.Get("X-Remote")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.responseBody))
			}))
			defer ts.Close()

			meta := &resource.Metadata{
				RemoteMixerDomain: ts.URL,
				RemoteMixerAPIKey: "test-api-key",
			}
			httpClient := ts.Client()

			req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}
			resp := &pbv2.NodeResponse{}

			err := FetchRemote(tc.ctx, meta, httpClient, "/v2/node", req, resp)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("FetchRemote() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("FetchRemote() error = %q, want containing %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("FetchRemote() unexpected error = %v", err)
			}

			if tc.checkHeader {
				if gotSurfaceHeader != tc.wantSurfaceHeader {
					t.Errorf("got X-Surface header %q, want %q", gotSurfaceHeader, tc.wantSurfaceHeader)
				}
				if gotRemoteHeader != tc.wantRemoteHeader {
					t.Errorf("got X-Remote header %q, want %q", gotRemoteHeader, tc.wantRemoteHeader)
				}
			}

			if tc.wantNodeName != "" {
				nodes := resp.GetData()["geoId/06"].GetArcs()["name"].GetNodes()
				if len(nodes) == 0 || nodes[0].GetName() != tc.wantNodeName {
					t.Errorf("FetchRemote() parsed resp = %v, want name %q", resp, tc.wantNodeName)
				}
			}
		})
	}
}
