package util

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
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
		want  bool
	}{
		{[]string{"abc", "geoId/12"}, true},
		{[]string{"a bc"}, false},
		{[]string{"abc "}, false},
		{[]string{"abc,efd"}, false},
	} {
		if got := CheckValidDCIDs(c.dcids); got != c.want {
			t.Errorf("CheckValidDCIDs(%v) = %v, want %v", c.dcids, got, c.want)
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
			&pb.GetPlacePageDataResponse{
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
			&pb.GetPlacePageDataResponse{
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
