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
	"fmt"
	"strconv"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed "WorldGeosForPlaceRecognition.csv"
var recogPlaceMapCSVContent []byte // Embed CSV as []byte.
//go:embed "WorldGeosForPlaceRecognitionAbbreviatedNames.csv"
var recogPlaceAbbreviatedNamesCSVContent []byte // Embed CSV as []byte.
//go:embed "BogusPlaceNames.csv"
var recogPlaceBogusPlaceNamesCSVContent []byte // Embed CSV as []byte.

// RecogPlaceStore contains data for recongizing places.
type RecogPlaceStore struct {
	// The key is the first token/word (lower case) of each place.
	RecogPlaceMap map[string]*pb.RecogPlaces
	// The key is abbreviated name of each place.
	AbbreviatedNameToPlaces map[string]*pb.RecogPlaces
	// If |resolve_description| is not set in RecognizePlacesRequest, bogus place names will not be
	// recognized unless they are followed by a containedInPlace.
	BogusPlaceNames map[string]struct{}
	// Place DCID to all possible names.
	DcidToNames map[string][]string
}

// LoadRecogPlaceStore loads RecogPlaceStore.
func LoadRecogPlaceStore() (*RecogPlaceStore, error) {
	reader := csv.NewReader(strings.NewReader(string(recogPlaceMapCSVContent)))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	dcidToAbbreviatedNames, err := loadAbbreviatedNames()
	if err != nil {
		return nil, err
	}

	bogusPlaceNames, err := loadBogusPlaceNames()
	if err != nil {
		return nil, err
	}

	recogPlaceMap := map[string]*pb.RecogPlaces{}
	abbreviatedNameToPlaces := map[string]*pb.RecogPlaces{}
	dcidToNames := map[string][]string{}
	expandedDcidToNames := map[string][]string{}
	dcidToContainingPlaces := map[string][]string{}
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
				"Wrong WorldGeosForPlaceRecognition CSV record: %v", record)
		}

		// DCID.
		dcid := strings.TrimSpace(record[0])
		if dcid == "" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Empty DCID for CSV record: %v", record)
		}
		recogPlace := &pb.RecogPlace{Dcid: dcid}

		// Containing places.
		containingPlaces := strings.Split(strings.ReplaceAll(record[3], " ", ""), ",")
		recogPlace.ContainingPlaces = containingPlaces
		dcidToContainingPlaces[dcid] = containingPlaces

		// Names.
		if strings.TrimSpace(record[2]) == "" {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Empty names for CSV record: %v", record)
		}
		names := strings.Split(strings.TrimSpace(record[2]), ",")
		dcidToNames[dcid] = names
		expandedDcidToNames[dcid] = names
		for _, name := range names {
			nameParts := strings.Split(name, " ")
			if len(nameParts) == 0 {
				return nil, status.Errorf(codes.FailedPrecondition,
					"Empty name parts in CSV record: %v", record)
			}
			nameMsg := &pb.RecogPlace_Name{}
			for _, namePart := range nameParts {
				nameMsg.Parts = append(nameMsg.Parts,
					strings.ToLower(strings.TrimSpace(namePart)))
			}
			recogPlace.Names = append(recogPlace.Names, nameMsg)
		}

		// Population.
		population, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Wrong population for CSV record: %v", record)
		}
		recogPlace.Population = population

		for _, name := range recogPlace.Names {
			key := name.Parts[0]
			if _, ok := recogPlaceMap[key]; !ok {
				recogPlaceMap[key] = &pb.RecogPlaces{}
			}
			recogPlaceMap[key].Places = append(recogPlaceMap[key].Places, recogPlace)
		}

		// Abbreviated names.
		if abbreviatedNames, ok := dcidToAbbreviatedNames[dcid]; ok {
			for _, aName := range abbreviatedNames {
				if _, ok := abbreviatedNameToPlaces[aName]; !ok {
					abbreviatedNameToPlaces[aName] = &pb.RecogPlaces{}
				}
				abbreviatedNameToPlaces[aName].Places = append(abbreviatedNameToPlaces[aName].Places,
					recogPlace)

				dcidToNames[dcid] = append(dcidToNames[dcid], aName)
				expandedDcidToNames[dcid] = append(expandedDcidToNames[dcid], aName)
			}
		}
	}

	// Add more names in the pattern of "selfName containingPlaceName", e.g., "Brussels Belgium".
	for dcid, selfNames := range dcidToNames {
		containingPlaces, ok := dcidToContainingPlaces[dcid]
		if !ok {
			continue
		}
		for _, containingPlace := range containingPlaces {
			containingPlaceNames, ok := dcidToNames[containingPlace]
			if !ok {
				continue
			}
			for _, containcontainingPlaceName := range containingPlaceNames {
				for _, selfName := range selfNames {
					expandedDcidToNames[dcid] = append(expandedDcidToNames[dcid],
						fmt.Sprintf("%s %s", selfName, containcontainingPlaceName))
				}
			}
		}
	}

	return &RecogPlaceStore{
		RecogPlaceMap:           recogPlaceMap,
		AbbreviatedNameToPlaces: abbreviatedNameToPlaces,
		BogusPlaceNames:         bogusPlaceNames,
		DcidToNames:             expandedDcidToNames,
	}, nil
}

func loadAbbreviatedNames() (map[string][]string, error) {
	reader := csv.NewReader(strings.NewReader(string(recogPlaceAbbreviatedNamesCSVContent)))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	res := map[string][]string{}

	isFirst := true
	for _, record := range records {
		// Skip header.
		if isFirst {
			isFirst = false
			continue
		}

		// Columns: dcid, abbreviatedNames.
		if len(record) != 2 {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Wrong WorldGeosForPlaceRecognitionAbbreviatedNames CSV record: %v", record)
		}

		dcid := record[0]
		abbreviatedNames := strings.Split(record[1], ",")
		if len(abbreviatedNames) == 0 {
			return nil, status.Errorf(codes.FailedPrecondition,
				"No abbreviatedNames: %v", record[1])
		}

		if _, ok := res[dcid]; !ok {
			res[dcid] = []string{}
		}
		res[dcid] = append(res[dcid], abbreviatedNames...)
	}

	return res, nil
}

func loadBogusPlaceNames() (map[string]struct{}, error) {
	reader := csv.NewReader(strings.NewReader(string(recogPlaceBogusPlaceNamesCSVContent)))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	res := map[string]struct{}{}

	for _, record := range records {
		if len(record) != 1 {
			return nil, status.Errorf(codes.FailedPrecondition,
				"Wrong BogusPlaceNames CSV record: %v", record)
		}
		res[strings.ToLower(strings.TrimSpace(record[0]))] = struct{}{}
	}

	return res, nil
}
