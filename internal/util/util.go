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
	"hash/fnv"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// LimitFactor is the amount to multiply the limit by to make sure certain
	// triples are returned by the BQ query.
	LimitFactor = 1
	// TextType represents text type.
	TextType = "Text"
	// String to represent in arc direction
	DirectionIn = "in"
	// String to represent out arc direction
	DirectionOut = "out"
)

// PlaceStatVar holds a place and a stat var dcid.
type PlaceStatVar struct {
	Place   string
	StatVar string
}

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

// SamplingStrategy represents the strategy to sample a JSON object.
//
// Sampling is performed uniform acroos the items for list, or the keys for
// map.For example, when MaxSample=4, sampling of [1,2,3,4,5,6,7,8,9]
// would give [3,5,7,9]
type SamplingStrategy struct {
	// Maximum number of samples.
	//
	// -1 means sample all the data. A positive integer indicates the maximum
	// number of samples.
	MaxSample int
	// Sampling strategy for the child fields.
	Children map[string]*SamplingStrategy
	// Proto fields or map keys that are not sampled at all.
	Exclude []string
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
	gzResult, err := io.ReadAll(gzReader)
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
	typeRelationJSON, err := os.ReadFile(typeRelationJSONFilePath)
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
			result += cases.Title(language.Und, cases.NoLower).String(p)
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
		if dcid == "" || strings.Contains(dcid, " ") || strings.Contains(dcid, ",") {
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

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// MergeDedupe merges a list of string lists and remove duplicate elements.
func MergeDedupe(strLists ...[]string) []string {
	if l := len(strLists); l == 0 {
		return []string{}
	} else if l == 1 {
		return strLists[0]
	}
	m := map[string]struct{}{}
	result := strLists[0]
	for i, strList := range strLists {
		for _, str := range strList {
			if i == 0 {
				// Initialize the set map.
				m[str] = struct{}{}
			} else {
				if _, ok := m[str]; !ok {
					result = append(result, str)
					m[str] = struct{}{}
				}
			}
		}
	}
	sort.Strings(result)
	return result
}

// Sample constructs a sampled protobuf message based on the sampling strategy.
// The output is deterministic given the same strategy.
func Sample(m proto.Message, strategy *SamplingStrategy) proto.Message {
	pr := m.ProtoReflect()
	pr.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		fieldName := fd.JSONName()

		// Clear the excluded fields
		for _, ex := range strategy.Exclude {
			if ex == fieldName {
				pr.Clear(fd)
				return true
			}
		}

		// If a field is not in the sampling strategy, keep it.
		strat, ok := strategy.Children[fieldName]
		if !ok {
			return true
		}
		// Note, map[string]proto.Message is treated as protoreflect.MessageKind,
		// So here need to check field and list first.
		if fd.IsList() {
			// Sample list.
			oldList := value.List()
			length := oldList.Len()
			var newList protoreflect.List

			maxSample := strat.MaxSample
			if strat.MaxSample == -1 || strat.MaxSample > length {
				maxSample = length
			}
			inc := 1
			if length > maxSample {
				inc = int(math.Ceil(float64(length) / float64(maxSample)))
			}
			// Get the latest data first
			for i := 0; i < maxSample; i++ {
				ind := length - 1 - i*inc
				if ind < 0 {
					break
				}
				newList.Append(oldList.Get(ind))
			}
			pr.Set(fd, protoreflect.ValueOfList(newList))
		} else if fd.IsMap() {
			currMap := value.Map()
			// Get all the keys
			allKeys := []string{}
			currMap.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				// Excluded keys
				for _, ex := range strat.Exclude {
					if ex == k.String() {
						return true
					}
				}
				allKeys = append(allKeys, k.String())
				return true
			})
			// Sort the keys
			sort.Strings(allKeys)
			// Sample keys
			sampleKeys := map[string]struct{}{}

			maxSample := strat.MaxSample
			if strat.MaxSample == -1 || strat.MaxSample > len(allKeys) {
				maxSample = len(allKeys)
			}
			inc := 1
			if len(allKeys) > maxSample {
				inc = int(math.Ceil(float64(len(allKeys)) / float64(maxSample)))
			}
			for i := 0; i < maxSample; i++ {
				ind := len(allKeys) - 1 - i*inc
				if ind < 0 {
					break
				}
				sampleKeys[allKeys[ind]] = struct{}{}
			}
			// Clear un-sampled entries
			currMap.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				if _, ok := sampleKeys[k.String()]; !ok {
					currMap.Clear(k)
				}
				return true
			})
			// If there are children strategy in a map, then apply this strategy to
			// each value of the map.
			if len(strat.Children) > 0 {
				currMap.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
					Sample(v.Message().Interface(), strat)
					return true
				})
			}

			// Set the map
			pr.Set(fd, protoreflect.ValueOfMap(currMap))
		} else if fd.Kind() == protoreflect.MessageKind {
			Sample(value.Message().Interface(), strat)
		}
		return true
	})
	return m
}

// TimeTrack is used to track function execution time.
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

// KeysToSlice stores the keys of a map in a slice.
func KeysToSlice(m map[string]bool) []string {
	s := make([]string, len(m))
	i := 0
	for k := range m {
		s[i] = k
		i++
	}
	sort.Strings(s)
	return s
}

// GetMetadataHash retrieves a hash string for a given protobuf message.
// Note this should be restrict to a request scope.
func GetMetadataHash(m *pb.StatMetadata) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.Join([]string{
		m.ImportName,
		m.MeasurementMethod,
		m.ObservationPeriod,
		m.ScalingFactor,
		m.Unit,
	}, "-")))
	return fmt.Sprint(h.Sum32())
}

// EncodeProto encodes a protobuf message into a compressed string
func EncodeProto(m proto.Message) (string, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}
	return ZipAndEncode(data)
}

// StringListIntersection finds common strings among a list of string lists.
// For example, for input [[a,b,c],[a,c,d],[a,c,e]], it returns [a,c].
func StringListIntersection(list [][]string) []string {
	uniqueStringSet := map[string]struct{}{}
	listOfStringSets := []map[string]struct{}{}
	for _, strs := range list {
		strSet := map[string]struct{}{}
		for _, str := range strs {
			uniqueStringSet[str] = struct{}{}
			strSet[str] = struct{}{}
		}
		listOfStringSets = append(listOfStringSets, strSet)
	}

	res := []string{}
	for str := range uniqueStringSet {
		isCommonStr := true
		for _, set := range listOfStringSets {
			if _, ok := set[str]; !ok {
				isCommonStr = false
				break
			}
		}
		if isCommonStr {
			res = append(res, str)
		}
	}
	sort.Strings(res)

	return res
}
