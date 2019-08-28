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

package util

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// BqPrefix for internal dataset.
	BqPrefix = "google.com:"
	// BtPropValInPrefix for internal GetPropertyValues in arc cache.
	BtPropValInPrefix = "d/0/"
	// BtPropValOutPrefix for internal GetPropertyValues out arc cache.
	BtPropValOutPrefix = "d/1/"
	// BtPopObsPrefix for internal pop obs cache.
	BtPopObsPrefix = "d/2/"
	// BtPlaceObsPrefix for internal place obs cache.
	BtPlaceObsPrefix = "d/3/"
	// BtObsAncestorPrefix for the ancestor node of Bigtable.
	BtObsAncestorPrefix = "d/6/"
	// BtTriplesPrefix for internal GetTriples cache.
	BtTriplesPrefix = "d/7/"
	// BtArcsPrefix for internal arcs cache.
	BtArcsPrefix = "d/9/"
	// BtPopPrefix for population cache.
	BtPopPrefix = "d/a/"
	// BtObsPrefix for observation cache.
	BtObsPrefix = "d/b/"
	// BtPlacesInPrefix for GetPlacesIn cache.
	BtPlacesInPrefix = "d/c/"
	// BtPopPVPrefix for population PVs.
	BtPopPVPrefix = "d/d/"
	// BtFamily is the key for the row.
	BtFamily = "csv"
	// BtCacheLimit is the cache limit. The limit is per predicate and neighbor type.
	BtCacheLimit = 500
	// BtBatchQuerySize is the size of BigTable batch query.
	BtBatchQuerySize = 1000
	// LimitFactor is the amount to multiply the limit by to make sure certain
	// triples are returned by the BQ query.
	LimitFactor = 1
	// TextType represents text type.
	TextType = "Text"
)

type typeInfo struct {
	Predicate string `json:"predicate"`
	SubType   string `json:"subType"`
	ObjType   string `json:"objType"`
}

// TypePair represents two types that are related.
type TypePair struct {
	Child  string
	Parent string
}

// GetProjectID gets the bigquery project id based on dataset name.
func GetProjectID(db string) (string, error) {
	if strings.HasPrefix(db, BqPrefix) {
		parts := strings.SplitN(strings.Replace(db, BqPrefix, "", 1), ".", 2)
		if len(parts) != 2 {
			return "", status.Errorf(
				codes.InvalidArgument, "Bad bigquery database name %s", db)
		}
		return BqPrefix + parts[0], nil
	}
	parts := strings.SplitN(db, ".", 2)
	if len(parts) != 2 {
		return "", status.Errorf(
			codes.InvalidArgument, "Bad bigquery database name %s", db)
	}
	return parts[0], nil
}

// ZipAndEncode Compresses the given contents using gzip and encodes it in base64
func ZipAndEncode(contents string) (string, error) {
	// Zip the string
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	_, err := gzWriter.Write([]byte(contents))
	if err != nil {
		return "", err
	}
	if err := gzWriter.Flush(); err != nil {
		return "", err
	}
	if err := gzWriter.Close(); err != nil {
		return "", err
	}

	// Encode using base64
	encode := base64.StdEncoding.EncodeToString(buf.Bytes())
	return encode, nil
}

// UnzipAndDecode decompresses the given contents using gzip and decodes it from base64
func UnzipAndDecode(contents string) ([]byte, error) {
	// Decode from base64
	decode, err := base64.StdEncoding.DecodeString(contents)
	if err != nil {
		return nil, err
	}

	// Unzip the string
	gzReader, err := gzip.NewReader(bytes.NewReader(decode))
	defer gzReader.Close()
	if err != nil {
		return nil, err
	}
	gzResult, err := ioutil.ReadAll(gzReader)
	if err != nil {
		return nil, err
	}

	return gzResult, nil
}

// StringList formats a list of strings into a comma-separated list with each surrounded
// with quotes.
func StringList(strs []string) string {
	strWithQuote := []string{}
	for _, s := range strs {
		strWithQuote = append(strWithQuote, fmt.Sprintf(`"%s"`, s))
	}
	return strings.Join(strWithQuote, ", ")
}

// StringContainedIn returns true if TARGET is contained in STRS
func StringContainedIn(target string, strs []string) bool {
	for _, s := range strs {
		if s == target {
			return true
		}
	}
	return false
}

// GetContainedIn returns the contained in relation change given two types.
func GetContainedIn(typeRelationJSONFilePath string) (map[TypePair][]string, error) {
	typeRelationJSON, err := ioutil.ReadFile(typeRelationJSONFilePath)
	if err != nil {
		return nil, err
	}

	ti := []typeInfo{}
	json.Unmarshal(typeRelationJSON, &ti)
	result := make(map[TypePair][]string)
	link := map[string][]string{}
	all := []string{}
	for _, info := range ti {
		if info.Predicate == "containedInPlace" {
			link[info.SubType] = append(link[info.SubType], info.ObjType)
			pair := TypePair{Child: info.SubType, Parent: info.ObjType}
			result[pair] = []string{}
			all = append(all, info.SubType, info.ObjType)
		}
	}
	for c, ps := range link {
		morep := ps
		for len(morep) > 0 {
			curr := morep[0]
			morep = morep[1:]
			for _, p := range link[curr] {
				result[TypePair{c, p}] = append(result[TypePair{c, curr}], curr)
				morep = append(morep, p)
			}
		}
	}
	return result, nil
}

// SnakeToCamel converts a snake case string to camel case string.
func SnakeToCamel(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}

	var result string
	parts := strings.Split(s, "_")
	capitalize := false
	for _, p := range parts {
		if p == "" {
			continue
		}

		if !capitalize {
			result += p
			capitalize = true
		} else {
			result += strings.Title(p)
		}
	}
	return result
}

var match = regexp.MustCompile("([a-z0-9])([A-Z])")

// CamelToSnake converts a camel case string to snake case string.
func CamelToSnake(str string) string {
	return strings.ToLower(match.ReplaceAllString(str, "${1}_${2}"))
}

// CheckValidDCIDs checks if DCIDs are valid. More criteria will be added as being discovered.
func CheckValidDCIDs(dcids []string) bool {
	for _, dcid := range dcids {
		if strings.Contains(dcid, " ") || strings.Contains(dcid, ",") {
			return false
		}
	}
	return true
}
