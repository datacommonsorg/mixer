package util

import (
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
		s1   []string
		s2   []string
		want []string
	}{
		{[]string{"abc", "geoId/12"}, []string{"abc"}, []string{"abc", "geoId/12"}},
		{[]string{"a", "bc"}, []string{"a", "bc", "d"}, []string{"a", "bc", "d"}},
		{[]string{"abc"}, []string{"ef"}, []string{"abc", "ef"}},
	} {
		got := MergeDedupe(c.s1, c.s2)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("MergeDedupe got diff %+v", diff)
		}
	}
}

func TestSampleJson(t *testing.T) {
	for _, c := range []struct {
		input    protoreflect.ProtoMessage
		expected protoreflect.ProtoMessage
		strategy *SamplingStrategy
	}{
		{
			&pb.GetLandingPageDataResponse{
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
									"2015": 1000,
									"2016": 2000,
								},
							},
						},
					},
					"geoId/06": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2019": 300,
									"2020": 400,
								},
							},
						},
					},
					"geoId/16": {
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
			&pb.GetLandingPageDataResponse{
				ChildPlacesType: "Country",
				ChildPlaces: []string{
					"geoId/12345",
					"geoId/54321",
				},
				StatVarSeries: map[string]*pb.StatVarSeries{
					"geoId/06": {
						Data: map[string]*pb.Series{
							"stat-var-1": {
								Val: map[string]float64{
									"2019": 300,
									"2020": 400,
								},
							},
						},
					},
				},
			},
			&SamplingStrategy{
				Children: map[string]*SamplingStrategy{
					"statVarSeries": {
						Ratio:   0.5,
						Exclude: []string{"country/USA"},
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
