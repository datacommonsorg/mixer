package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
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
