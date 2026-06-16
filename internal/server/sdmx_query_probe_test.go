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

package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestParseSDMXProbeRawQuery(t *testing.T) {
	queryParams, constraints, err := parseSDMXProbeRawQuery(
		"c%5BFREQ%5D=A,M&c[CONF_STATUS]=F&c[OBS_VALUE]=ge%3A10.0&" +
			"updatedAfter=2009-05-15T14%3A15%3A00%2B01%3A00&" +
			"firstNObservations=1&lastNObservations=2&dimensionAtObservation=AllDimensions&" +
			"attributes=OBS_STATUS,UNIT_MEASURE&measures=OBS_VALUE&includeHistory=true&" +
			"offset=10&limit=20&sort=series_key%3Adesc+TIME_PERIOD%3Aasc&" +
			"asOf=2009-06-15T14%3A15%3A00%2B01%3A00&reportingYearStartDay=--07-01&key=secret",
	)
	if err != nil {
		t.Fatalf("parseSDMXProbeRawQuery() returned error: %v", err)
	}

	wantQueryParams := map[string][]string{
		"c[FREQ]":                {"A,M"},
		"c[CONF_STATUS]":         {"F"},
		"c[OBS_VALUE]":           {"ge:10.0"},
		"updatedAfter":           {"2009-05-15T14:15:00+01:00"},
		"firstNObservations":     {"1"},
		"lastNObservations":      {"2"},
		"dimensionAtObservation": {"AllDimensions"},
		"attributes":             {"OBS_STATUS,UNIT_MEASURE"},
		"measures":               {"OBS_VALUE"},
		"includeHistory":         {"true"},
		"offset":                 {"10"},
		"limit":                  {"20"},
		"sort":                   {"series_key:desc+TIME_PERIOD:asc"},
		"asOf":                   {"2009-06-15T14:15:00+01:00"},
		"reportingYearStartDay":  {"--07-01"},
		"key":                    {"[REDACTED]"},
	}
	if diff := cmp.Diff(wantQueryParams, queryParams); diff != "" {
		t.Errorf("parseSDMXProbeRawQuery() query params mismatch (-want +got):\n%s", diff)
	}

	wantConstraints := map[string][]string{
		"FREQ":        {"A,M"},
		"CONF_STATUS": {"F"},
		"OBS_VALUE":   {"ge:10.0"},
	}
	if diff := cmp.Diff(wantConstraints, constraints); diff != "" {
		t.Errorf("parseSDMXProbeRawQuery() constraints mismatch (-want +got):\n%s", diff)
	}
}

func TestParseSDMXProbeRawQueryConstraintForms(t *testing.T) {
	queryParams, constraints, err := parseSDMXProbeRawQuery(
		"c%5BFREQ%5D=M&c[TIME_PERIOD]=ge%3A2020-01%2Ble%3A2020-12&" +
			"c[RAW_PERIOD]=ge:2020-01+le:2020-12&c[TITLE]=co%3Aoil%20and%20gas&" +
			"c[COUNTRY]=country%2FUSA&c[ATTR1]=A,B+M&c[PLUS_VALUE]=A%2BB",
	)
	if err != nil {
		t.Fatalf("parseSDMXProbeRawQuery() returned error: %v", err)
	}

	wantQueryParams := map[string][]string{
		"c[FREQ]":        {"M"},
		"c[TIME_PERIOD]": {"ge:2020-01+le:2020-12"},
		"c[RAW_PERIOD]":  {"ge:2020-01+le:2020-12"},
		"c[TITLE]":       {"co:oil and gas"},
		"c[COUNTRY]":     {"country/USA"},
		"c[ATTR1]":       {"A,B+M"},
		"c[PLUS_VALUE]":  {"A+B"},
	}
	if diff := cmp.Diff(wantQueryParams, queryParams); diff != "" {
		t.Errorf("parseSDMXProbeRawQuery() query params mismatch (-want +got):\n%s", diff)
	}

	wantConstraints := map[string][]string{
		"FREQ":        {"M"},
		"TIME_PERIOD": {"ge:2020-01+le:2020-12"},
		"RAW_PERIOD":  {"ge:2020-01+le:2020-12"},
		"TITLE":       {"co:oil and gas"},
		"COUNTRY":     {"country/USA"},
		"ATTR1":       {"A,B+M"},
		"PLUS_VALUE":  {"A+B"},
	}
	if diff := cmp.Diff(wantConstraints, constraints); diff != "" {
		t.Errorf("parseSDMXProbeRawQuery() constraints mismatch (-want +got):\n%s", diff)
	}
}

func TestV3SdmxQueryProbe(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		sdmxOriginalPathMetadataKey,
		"/sdmx/v3/debug/data/dataflow/AGENCY/FLOW/1.0.0/*?c[FREQ]=M&c[TIME_PERIOD]=ge:2015&c[TIME_PERIOD]=le:2020",
		"accept",
		"application/vnd.sdmx.data+json;version=2.0.0",
	))
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: true}}

	body, err := server.V3SdmxQueryProbe(ctx, &pbv3.SdmxQueryProbeRequest{
		Context:    "dataflow",
		AgencyId:   "AGENCY",
		ResourceId: "FLOW",
		Version:    "1.0.0",
		Key:        "*",
	})
	if err != nil {
		t.Fatalf("V3SdmxQueryProbe() returned error: %v", err)
	}
	if body.GetContentType() != "application/json; charset=utf-8" {
		t.Errorf("V3SdmxQueryProbe() content type = %q, want application/json; charset=utf-8", body.GetContentType())
	}

	var got sdmxQueryProbeResponse
	if err := json.Unmarshal(body.GetData(), &got); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}

	want := sdmxQueryProbeResponse{
		Path: "/sdmx/v3/debug/data/dataflow/AGENCY/FLOW/1.0.0/*",
		PathParams: map[string]string{
			"context":    "dataflow",
			"agencyID":   "AGENCY",
			"resourceID": "FLOW",
			"version":    "1.0.0",
			"key":        "*",
		},
		QueryParams: map[string][]string{
			"c[FREQ]":        {"M"},
			"c[TIME_PERIOD]": {"ge:2015", "le:2020"},
		},
		Constraints: map[string][]string{
			"FREQ":        {"M"},
			"TIME_PERIOD": {"ge:2015", "le:2020"},
		},
		Headers: map[string][]string{
			"accept": {"application/vnd.sdmx.data+json;version=2.0.0"},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("V3SdmxQueryProbe() response mismatch (-want +got):\n%s", diff)
	}
}

func TestV3SdmxQueryProbeRequiresOriginalPath(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: true}}
	_, err := server.V3SdmxQueryProbe(context.Background(), &pbv3.SdmxQueryProbeRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxQueryProbe() error code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}
