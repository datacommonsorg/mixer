// Copyright 2023 Google LLC
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

// Package files contains code for files.
package files

import (
	_ "embed"
	"encoding/csv"
	"strconv"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed "USGeosForPlaceRecognition.csv"
var recogPlaceMapCSVContent []byte // Embed CSV as []byte.

// RecogPlaceMap returns a map for RecogPlaces, the key is the first token/word of each place.
func RecogPlaceMap() (map[string]*pb.RecogPlaces, error) {
	reader := csv.NewReader(strings.NewReader(string(recogPlaceMapCSVContent)))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	recogPlaceMap := map[string]*pb.RecogPlaces{}
	isFirst := true
	for _, record := range records {
		// Skip header.
		if isFirst {
			isFirst = false
			continue
		}

		// Columns: dcid, mainType, name, linkedContainedInPlace, population.
		if len(record) != 5 {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Wrong RecogPlaces CSV record: %v", record)
		}

		recogPlace := &pb.RecogPlace{
			ContainingPlaces: strings.Split(strings.ReplaceAll(record[3], " ", ""), ","),
		}

		// DCID.
		recogPlace.Dcid = strings.TrimSpace(record[0])
		if recogPlace.Dcid == "" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Empty DCID for CSV record: %v", record)
		}

		// Name.
		nameParts := strings.Split(record[2], " ")
		if len(nameParts) == 0 {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Empty name parts for CSV record: %v", record)
		}
		for _, namePart := range nameParts {
			recogPlace.Words = append(recogPlace.Words,
				strings.ToLower(strings.TrimSpace(namePart)))
		}

		// Population.
		population, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Wrong population for CSV record: %v", record)
		}
		recogPlace.Population = population

		key := recogPlace.Words[0]
		if _, ok := recogPlaceMap[key]; !ok {
			recogPlaceMap[key] = &pb.RecogPlaces{}
		}
		recogPlaceMap[key].Places = append(recogPlaceMap[key].Places, recogPlace)
	}

	return recogPlaceMap, nil
}
