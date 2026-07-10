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

package spanner

import (
	"testing"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// ---------------------------------------------------------------------------
// parseDomain
// ---------------------------------------------------------------------------

func TestParseDomain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		url  string
		want string
	}{
		{"standard url", "https://www.example.com/page", "example.com"},
		{"no subdomain", "https://example.com/page", "example.com"},
		{"deep subdomain", "https://a.b.c.example.com/page", "example.com"},
		{"with port", "http://localhost:8080/path", "localhost"},
		{"invalid url", "not a url", ""},
		{"empty string", "", ""},
		{"just host", "https://example.com", "example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDomain(tc.url)
			if got != tc.want {
				t.Errorf("parseDomain(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseMagnitudeDcid
// ---------------------------------------------------------------------------

func TestParseMagnitudeDcid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		magnitudeDcid string
		unit          string
		want           float64
	}{
		{"standard", "SquareKilometer91.57871", "SquareKilometer", 91.57871},
		{"knot unit", "Knot45.5", "Knot", 45.5},
		{"celsius", "Celsius-10.0", "Celsius", -10.0},
		{"empty dcid", "", "SquareKilometer", 0.0},
		{"empty unit", "SquareKilometer100", "", 0.0},
		{"no match prefix", "FooBar100", "SquareKilometer", 0.0},
		{"invalid number", "SquareKilometerabc", "SquareKilometer", 0.0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseMagnitudeDcid(tc.magnitudeDcid, tc.unit)
			if got != tc.want {
				t.Errorf("parseMagnitudeDcid(%q, %q) = %v, want %v", tc.magnitudeDcid, tc.unit, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseAndSortEvents
// ---------------------------------------------------------------------------

func TestParseAndSortEvents_DescByMagnitude(t *testing.T) {
	t.Parallel()

	rows := []EventIdWithMagnitudeDcid{
		{EventID: "event/A", MagnitudeDcid: "SquareKilometer50.0"},
		{EventID: "event/B", MagnitudeDcid: "SquareKilometer100.0"},
		{EventID: "event/C", MagnitudeDcid: "SquareKilometer50.0"},
	}

	got := parseAndSortEvents(rows, "FireEvent")
	want := []string{"event/B", "event/A", "event/C"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("parseAndSortEvents() diff (-want +got):\n%s", diff)
	}
}

func TestParseAndSortEvents_AscByMagnitude(t *testing.T) {
	t.Parallel()

	// ColdTemperatureEvent uses ASC order.
	rows := []EventIdWithMagnitudeDcid{
		{EventID: "event/A", MagnitudeDcid: "Celsius10.0"},
		{EventID: "event/B", MagnitudeDcid: "Celsius5.0"},
		{EventID: "event/C", MagnitudeDcid: "Celsius15.0"},
	}

	got := parseAndSortEvents(rows, "ColdTemperatureEvent")
	want := []string{"event/B", "event/A", "event/C"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("parseAndSortEvents() diff (-want +got):\n%s", diff)
	}
}

func TestParseAndSortEvents_TieBreakerByDcid(t *testing.T) {
	t.Parallel()

	// Same magnitude: should be sorted alphabetically by DCID.
	rows := []EventIdWithMagnitudeDcid{
		{EventID: "event/Z", MagnitudeDcid: "SquareKilometer100.0"},
		{EventID: "event/A", MagnitudeDcid: "SquareKilometer100.0"},
		{EventID: "event/M", MagnitudeDcid: "SquareKilometer100.0"},
	}

	got := parseAndSortEvents(rows, "FireEvent")
	want := []string{"event/A", "event/M", "event/Z"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("parseAndSortEvents() diff (-want +got):\n%s", diff)
	}
}

func TestParseAndSortEvents_TruncatesToMaxEvents(t *testing.T) {
	t.Parallel()

	// Generate more than maxEvents (100) events.
	rows := make([]EventIdWithMagnitudeDcid, 150)
	for i := range rows {
		rows[i] = EventIdWithMagnitudeDcid{
			EventID:       "event/" + string(rune('A'+i%26)) + string(rune('0'+i/26)),
			MagnitudeDcid: "SquareKilometer100.0",
		}
	}

	got := parseAndSortEvents(rows, "FireEvent")
	if len(got) != maxEvents {
		t.Fatalf("len(got) = %d, want %d", len(got), maxEvents)
	}
}

func TestParseAndSortEvents_UnknownEventTypeNoMagnitude(t *testing.T) {
	t.Parallel()

	// Unknown event type: magnitudes are all 0, so ties broken by DCID.
	rows := []EventIdWithMagnitudeDcid{
		{EventID: "event/B", MagnitudeDcid: "SomeUnit50.0"},
		{EventID: "event/A", MagnitudeDcid: "SomeUnit100.0"},
	}

	got := parseAndSortEvents(rows, "UnknownEventType")
	want := []string{"event/A", "event/B"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("parseAndSortEvents() diff (-want +got):\n%s", diff)
	}
}

// ---------------------------------------------------------------------------
// populateSpecialFields
// ---------------------------------------------------------------------------

func TestPopulateSpecialFields(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		edges []*Edge
		want  *pbv1.EventCollection_Event
	}{
		{
			name: "affected place (non-s2cell)",
			edges: []*Edge{
				{Predicate: predAffectedPlace, Value: "geoId/06"},
			},
			want: &pbv1.EventCollection_Event{
				Places:       []string{"geoId/06"},
				Dates:        []string{},
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
				PropVals:     map[string]*pbv1.EventCollection_ValList{},
			},
		},
		{
			name: "affected place (s2cell excluded)",
			edges: []*Edge{
				{Predicate: predAffectedPlace, Value: "s2CellId/12345"},
			},
			want: &pbv1.EventCollection_Event{
				Places:       []string{},
				Dates:        []string{},
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
				PropVals:     map[string]*pbv1.EventCollection_ValList{},
			},
		},
		{
			name: "start date",
			edges: []*Edge{
				{Predicate: predStartDate, Value: "2020-10"},
			},
			want: &pbv1.EventCollection_Event{
				Places:       []string{},
				Dates:        []string{"2020-10"},
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
				PropVals:     map[string]*pbv1.EventCollection_ValList{},
			},
		},
		{
			name: "non-special predicate ignored",
			edges: []*Edge{
				{Predicate: "magnitude", Value: "50.0"},
			},
			want: &pbv1.EventCollection_Event{
				Places:       []string{},
				Dates:        []string{},
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
				PropVals:     map[string]*pbv1.EventCollection_ValList{},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := &pbv1.EventCollection_Event{
				Places:       []string{},
				Dates:        []string{},
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
				PropVals:     make(map[string]*pbv1.EventCollection_ValList),
			}
			for _, edge := range tc.edges {
				populateSpecialFields(event, edge)
			}
			if diff := cmp.Diff(tc.want, event, protocmp.Transform()); diff != "" {
				t.Fatalf("populateSpecialFields() diff (-want +got):\n%s", diff)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// populateGeoLocation
// ---------------------------------------------------------------------------

func TestPopulateGeoLocation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value string
		want  int // number of geo locations expected
	}{
		{"valid latlong", "latLong/577521_-958960", 1},
		{"negative coords", "latLong/-577521_-958960", 1},
		{"invalid format", "geoId/06", 0},
		{"empty string", "", 0},
		{"latlong missing second coord", "latLong/577521", 0},
		{"latlong non-numeric", "latLong/abc_def", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := &pbv1.EventCollection_Event{
				GeoLocations: []*pbv1.EventCollection_GeoLocation{},
			}
			populateGeoLocation(event, tc.value)
			if len(event.GeoLocations) != tc.want {
				t.Fatalf("len(GeoLocations) = %d, want %d", len(event.GeoLocations), tc.want)
			}
		})
	}
}

func TestPopulateGeoLocation_ParsesCoordinates(t *testing.T) {
	t.Parallel()

	event := &pbv1.EventCollection_Event{
		GeoLocations: []*pbv1.EventCollection_GeoLocation{},
	}
	// latLong/577521_-958960 -> lat=5.77521, lon=-9.58960
	populateGeoLocation(event, "latLong/577521_-958960")

	if len(event.GeoLocations) != 1 {
		t.Fatalf("len(GeoLocations) = %d, want 1", len(event.GeoLocations))
	}
	point := event.GeoLocations[0].GetPoint()
	if point.GetLatitude() < 5.77 || point.GetLatitude() > 5.78 {
		t.Errorf("Latitude = %v, want ~5.77521", point.GetLatitude())
	}
	if point.GetLongitude() < -9.59 || point.GetLongitude() > -9.58 {
		t.Errorf("Longitude = %v, want ~-9.5896", point.GetLongitude())
	}
}

// ---------------------------------------------------------------------------
// populatePropVals
// ---------------------------------------------------------------------------

func TestPopulatePropVals(t *testing.T) {
	t.Parallel()

	t.Run("string value", func(t *testing.T) {
		event := &pbv1.EventCollection_Event{
			PropVals: make(map[string]*pbv1.EventCollection_ValList),
		}
		populatePropVals(event, &Edge{Predicate: "magnitude", Value: "50.0"})

		if len(event.PropVals["magnitude"].Vals) != 1 {
			t.Fatalf("len(Vals) = %d, want 1", len(event.PropVals["magnitude"].Vals))
		}
		if got := event.PropVals["magnitude"].Vals[0]; got != "50.0" {
			t.Errorf("Vals[0] = %q, want %q", got, "50.0")
		}
	})

	t.Run("provenance set from edge", func(t *testing.T) {
		event := &pbv1.EventCollection_Event{
			PropVals: make(map[string]*pbv1.EventCollection_ValList),
		}
		populatePropVals(event, &Edge{Predicate: "magnitude", Value: "50.0", Provenance: "dc/base/test"})
		if event.ProvenanceId != "dc/base/test" {
			t.Errorf("ProvenanceId = %q, want %q", event.ProvenanceId, "dc/base/test")
		}
	})

	t.Run("multiple values for same predicate", func(t *testing.T) {
		event := &pbv1.EventCollection_Event{
			PropVals: make(map[string]*pbv1.EventCollection_ValList),
		}
		populatePropVals(event, &Edge{Predicate: "color", Value: "red"})
		populatePropVals(event, &Edge{Predicate: "color", Value: "blue"})

		if len(event.PropVals["color"].Vals) != 2 {
			t.Fatalf("len(Vals) = %d, want 2", len(event.PropVals["color"].Vals))
		}
	})

	t.Run("geojson raw bytes used as value", func(t *testing.T) {
		// Use a non-gzipped byte slice (won't match the gzip magic bytes 0x1f 0x8b).
		// This tests the fallback: val = string(edge.Bytes).
		event := &pbv1.EventCollection_Event{
			PropVals: make(map[string]*pbv1.EventCollection_ValList),
		}
		rawJSON := `{"type":"Polygon","coordinates":[[0,0]]}`
		edge := &Edge{
			Predicate: predGeoJsonCoordinates,
			Value:     "ignored",
			Bytes:     []byte(rawJSON),
		}
		populatePropVals(event, edge)

		if got := event.PropVals[predGeoJsonCoordinates].Vals[0]; got != rawJSON {
			t.Errorf("Vals[0] = %q, want %q", got, rawJSON)
		}
	})
}

// ---------------------------------------------------------------------------
// cleanUpPropVals
// ---------------------------------------------------------------------------

func TestCleanUpPropVals(t *testing.T) {
	t.Parallel()

	event := &pbv1.EventCollection_Event{
		PropVals: map[string]*pbv1.EventCollection_ValList{
			predAffectedPlace:  {Vals: []string{"geoId/06"}},
			predStartDate:       {Vals: []string{"2020-10"}},
			predStartLocation:   {Vals: []string{"latLong/0_0"}},
			predProvenance:      {Vals: []string{"dc/base/test"}},
			predTypeOf:          {Vals: []string{"FireEvent"}},
			"magnitude":         {Vals: []string{"50.0"}},
		},
	}

	cleanUpPropVals(event)

	for _, key := range []string{predAffectedPlace, predStartDate, predStartLocation, predProvenance, predTypeOf} {
		if _, ok := event.PropVals[key]; ok {
			t.Errorf("PropVals[%q] still present after cleanup", key)
		}
	}
	if _, ok := event.PropVals["magnitude"]; !ok {
		t.Error("PropVals[\"magnitude\"] was removed but should have been kept")
	}
}

// ---------------------------------------------------------------------------
// keepEvent
// ---------------------------------------------------------------------------

func TestKeepEvent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		req   *pbv1.EventCollectionRequest
		event *pbv1.EventCollection_Event
		want  bool
	}{
		{
			name:  "no filter prop keeps all",
			req:   &pbv1.EventCollectionRequest{},
			event: &pbv1.EventCollection_Event{},
			want:  true,
		},
		{
			name: "value within range",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"area": {Vals: []string{"SquareKilometer150"}},
				},
			},
			want: true,
		},
		{
			name: "value below range",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"area": {Vals: []string{"SquareKilometer50"}},
				},
			},
			want: false,
		},
		{
			name: "value above range",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"area": {Vals: []string{"SquareKilometer250"}},
				},
			},
			want: false,
		},
		{
			name: "filter prop not in event",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"magnitude": {Vals: []string{"50.0"}},
				},
			},
			want: false,
		},
		{
			name: "empty vals for filter prop",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"area": {Vals: []string{}},
				},
			},
			want: false,
		},
		{
			name: "non-numeric value",
			req: &pbv1.EventCollectionRequest{
				FilterProp:       "area",
				FilterUnit:       "SquareKilometer",
				FilterLowerLimit: 100,
				FilterUpperLimit: 200,
			},
			event: &pbv1.EventCollection_Event{
				PropVals: map[string]*pbv1.EventCollection_ValList{
					"area": {Vals: []string{"SquareKilometerabc"}},
				},
			},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := keepEvent(tc.event, tc.req)
			if got != tc.want {
				t.Errorf("keepEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// assembleAndFilterEvent
// ---------------------------------------------------------------------------

func TestAssembleAndFilterEvent(t *testing.T) {
	t.Parallel()

	t.Run("assembles event from edges and keeps without filter", func(t *testing.T) {
		edges := []*Edge{
			{Predicate: predAffectedPlace, Value: "geoId/06"},
			{Predicate: predStartDate, Value: "2020-10"},
			{Predicate: "magnitude", Value: "50.0", Provenance: "dc/base/test"},
		}
		req := &pbv1.EventCollectionRequest{}

		event := assembleAndFilterEvent("event/123", edges, req)
		if event == nil {
			t.Fatal("assembleAndFilterEvent() = nil, want non-nil")
		}
		if event.Dcid != "event/123" {
			t.Errorf("Dcid = %q, want %q", event.Dcid, "event/123")
		}
		if len(event.Places) != 1 || event.Places[0] != "geoId/06" {
			t.Errorf("Places = %v, want [geoId/06]", event.Places)
		}
		if len(event.Dates) != 1 || event.Dates[0] != "2020-10" {
			t.Errorf("Dates = %v, want [2020-10]", event.Dates)
		}
		if event.ProvenanceId != "dc/base/test" {
			t.Errorf("ProvenanceId = %q, want %q", event.ProvenanceId, "dc/base/test")
		}
		// Specialized fields should be cleaned from PropVals.
		if _, ok := event.PropVals[predAffectedPlace]; ok {
			t.Error("predAffectedPlace should have been cleaned from PropVals")
		}
		if _, ok := event.PropVals[predStartDate]; ok {
			t.Error("predStartDate should have been cleaned from PropVals")
		}
		// Non-specialized field should remain.
		if _, ok := event.PropVals["magnitude"]; !ok {
			t.Error("PropVals[\"magnitude\"] should have been retained")
		}
	})

	t.Run("filters out event that does not match", func(t *testing.T) {
		edges := []*Edge{
			{Predicate: "area", Value: "SquareKilometer50"},
		}
		req := &pbv1.EventCollectionRequest{
			FilterProp:       "area",
			FilterUnit:       "SquareKilometer",
			FilterLowerLimit: 100,
			FilterUpperLimit: 200,
		}

		event := assembleAndFilterEvent("event/456", edges, req)
		if event != nil {
			t.Fatalf("assembleAndFilterEvent() = %v, want nil", event)
		}
	})

	t.Run("nil edges produce empty event", func(t *testing.T) {
		req := &pbv1.EventCollectionRequest{}
		event := assembleAndFilterEvent("event/789", nil, req)
		if event == nil {
			t.Fatal("assembleAndFilterEvent() = nil, want non-nil")
		}
		if event.Dcid != "event/789" {
			t.Errorf("Dcid = %q, want %q", event.Dcid, "event/789")
		}
	})
}

// ---------------------------------------------------------------------------
// assembleEventCollection
// ---------------------------------------------------------------------------

func TestAssembleEventCollection(t *testing.T) {
	t.Parallel()

	dcids := []string{"event/A", "event/B", "event/C"}
	edgesMap := map[string][]*Edge{
		"event/A": {
			{Predicate: predStartDate, Value: "2020-01"},
			{Predicate: "magnitude", Value: "100.0"},
		},
		"event/B": {
			{Predicate: predStartDate, Value: "2020-02"},
		},
		// event/C has no edges in the map.
	}
	req := &pbv1.EventCollectionRequest{}

	res := assembleEventCollection(dcids, edgesMap, req)

	if len(res.Events) != 2 {
		t.Fatalf("len(Events) = %d, want 2", len(res.Events))
	}
	if res.Events[0].Dcid != "event/A" {
		t.Errorf("Events[0].Dcid = %q, want %q", res.Events[0].Dcid, "event/A")
	}
	if res.Events[1].Dcid != "event/B" {
		t.Errorf("Events[1].Dcid = %q, want %q", res.Events[1].Dcid, "event/B")
	}
	if res.ProvenanceInfo == nil {
		t.Error("ProvenanceInfo should be initialized")
	}
}
