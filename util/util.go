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
	"math/rand"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// BtPlaceStatsVarPrefix for place to statsvar list cache.
	BtPlaceStatsVarPrefix = "d/0/"
	// BtPopObsPrefix for internal pop obs cache.
	BtPopObsPrefix = "d/2/"
	// BtPlaceObsPrefix for internal place obs cache.
	BtPlaceObsPrefix = "d/3/"
	// BtLandingPagePrefix for place landing page cache.
	BtLandingPagePrefix = "d/4/"
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
	// BtChartDataPrefix for chart data.
	BtChartDataPrefix = "d/f/"
	// BtInPropValPrefix for in-arc prop value.
	BtInPropValPrefix = "d/l/"
	// BtOutPropValPrefix for out-arc prop value.
	BtOutPropValPrefix = "d/m/"
	// BtRelatedLocationsSameTypePrefix for related places with same type.
	BtRelatedLocationsSameTypePrefix = "d/o/"
	// BtRelatedLocationsSameTypeAndAncestorPrefix for related places with same type and ancestor.
	BtRelatedLocationsSameTypeAndAncestorPrefix = "d/q/"
	// BtRelatedLocationsSameTypePCPrefix for related places with same type, per capita.
	BtRelatedLocationsSameTypePCPrefix = "d/o0/"
	// BtRelatedLocationsSameTypeAndAncestorPCPrefix for related places with same type and ancestor,
	// per capita.
	BtRelatedLocationsSameTypeAndAncestorPCPrefix = "d/q0/"

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

// ZipAndEncode Compresses the given contents using gzip and encodes it in base64
func ZipAndEncode(contents []byte) (string, error) {
	// Zip the string
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	_, err := gzWriter.Write(contents)
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
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()
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
	err = json.Unmarshal(typeRelationJSON, &ti)
	if err != nil {
		return nil, err
	}
	result := make(map[TypePair][]string)
	link := map[string][]string{}
	for _, info := range ti {
		if info.Predicate == "containedInPlace" {
			link[info.SubType] = append(link[info.SubType], info.ObjType)
			pair := TypePair{Child: info.SubType, Parent: info.ObjType}
			result[pair] = []string{}
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

// RandomString creates a random string with 16 runes.
func RandomString() string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")
	length := 16
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

var re = regexp.MustCompile(`(.+?)\/(.+?)\/(.+)`)

// KeyToDcid ...
// The Bigtable key is in the form of "x/y/dcid^prop1^prop2^..."
func KeyToDcid(key string) (string, error) {
	parts := strings.Split(key, "^")
	match := re.FindStringSubmatch(parts[0])
	if len(match) != 4 {
		return "", status.Errorf(codes.Internal, "Invalid bigtable row key %s", key)
	}
	return match[3], nil
}

// RemoveKeyPrefix removes the prefix of a big query key
func RemoveKeyPrefix(key string) (string, error) {
	match := re.FindStringSubmatch(key)
	if len(match) != 4 {
		return "", status.Errorf(codes.Internal, "Invalid bigtable row key %s", key)
	}
	return match[3], nil
}
