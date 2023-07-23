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

package writer

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	observationsHeader = []string{"entity", "variable", "date", "value"}
	triplesHeader      = []string{"subject_id", "predicate", "object_id", "object_value"}
)

type observation struct {
	entity   string
	variable string
	date     string
	value    string
}

type triple struct {
	subjectID   string
	predicate   string
	objectID    string
	objectValue string
}

type context struct {
	fileDir          string
	resourceMetadata *resource.Metadata
}

// Write writes raw CSV files to SQLite CSV files.
func WriteCSV(resourceMetadata *resource.Metadata) error {
	fileDir := resourceMetadata.SQLitePath
	csvFiles, err := listCSVFiles(fileDir)
	if err != nil {
		return err
	}
	if len(csvFiles) == 0 {
		return status.Errorf(codes.FailedPrecondition, "No CSV files found in %s", fileDir)
	}

	observationList := []*observation{}
	variableSet := map[string]struct{}{}
	for _, csvFile := range csvFiles {
		observations, variables, err := processCSVFile(&context{
			fileDir:          fileDir,
			resourceMetadata: resourceMetadata,
		}, csvFile)
		if err != nil {
			return err
		}
		observationList = append(observationList, observations...)
		for _, v := range variables {
			variableSet[v] = struct{}{}
		}
	}

	tripleList := []*triple{}
	tripleList = append(
		tripleList,
		&triple{
			subjectID: "dc/g/New",
			predicate: "typeOf",
			objectID:  "StatVarGroup",
		},
		&triple{
			subjectID:   "dc/g/New",
			predicate:   "name",
			objectValue: "New Variables",
		},
		&triple{
			subjectID: "dc/g/New",
			predicate: "specializationOf",
			objectID:  "dc/g/Root",
		},
	)

	for variable := range variableSet {
		tripleList = append(
			tripleList,
			&triple{
				subjectID: variable,
				predicate: "typeOf",
				objectID:  "StatisticalVariable",
			},
			&triple{
				subjectID: variable,
				predicate: "memberOf",
				objectID:  "dc/g/New",
			},
			&triple{
				subjectID:   variable,
				predicate:   "description",
				objectValue: variable,
			},
		)
	}

	return writeOutput(observationList, tripleList, path.Join(fileDir, "internal"))
}

func WriteSQLite(fileDir string) error {
	script := fmt.Sprintf(`
sqlite3 %s <<EOF
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS triples;
.headers on
.mode csv
.import %s observations
.import %s triples
EOF`,
		path.Join(fileDir, "datacommons.db"),
		path.Join(fileDir, "internal", "observations.csv"),
		path.Join(fileDir, "internal", "triples.csv"),
	)
	cmd := exec.Command(
		"bash",
		"-c",
		script,
	)
	return cmd.Run()
}

func listCSVFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, file := range files {
		if fName := file.Name(); strings.HasSuffix(fName, ".csv") {
			res = append(res, file.Name())
		}
	}

	return res, nil
}

func processCSVFile(ctx *context, csvFile string) ([]*observation,
	[]string, // A list of variables.
	error) {
	// Read the CSV file.
	f, err := os.Open(filepath.Join(ctx.fileDir, csvFile))
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, nil, err
	}
	numRecords := len(records)
	if numRecords < 2 {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"Empty CSV file %s", csvFile)
	}

	// Load header.
	header := records[0]
	if len(header) < 3 {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"Less than 3 columns in CSV file %s", csvFile)
	}
	numColumns := len(header)

	// Resolve places.
	places := []string{}
	for i := 1; i < numRecords; i++ {
		places = append(places, records[i][0])
	}
	resolvedPlaceMap, err := resolvePlaces(ctx, places, header[0])
	if err != nil {
		return nil, nil, err
	}

	// Generate observations.
	observations := []*observation{}
	for i := 1; i < numRecords; i++ {
		record := records[i]

		resolvedPlace, ok := resolvedPlaceMap[record[0]]
		if !ok {
			// If a place cannot be resolved, simply ignore it.
			continue
		}

		for j := 2; j < numColumns; j++ {
			observations = append(observations, &observation{
				entity:   resolvedPlace,
				variable: header[j],
				date:     record[1],
				value:    record[j],
			})
		}
	}

	return observations, header[2:], nil
}

func resolvePlaces(ctx *context,
	places []string,
	placeHeader string) (map[string]string, error) {
	placeToDCID := map[string]string{}

	if placeHeader == "lat#lng" {
		// TODO(ws): lat#lng recon.
	} else if placeHeader == "name" {
		// TODO(ws): name recon.
	} else {
		resp := &pbv2.ResolveResponse{}
		httpClient := &http.Client{}
		if err := util.FetchRemote(ctx.resourceMetadata, httpClient, "/v2/resolve",
			&pbv2.ResolveRequest{
				Nodes:    places,
				Property: fmt.Sprintf("<-%s->dcid", placeHeader),
			}, resp); err != nil {
			return nil, err
		}
		for _, entity := range resp.GetEntities() {
			if _, ok := placeToDCID[entity.GetNode()]; ok {
				continue
			}
			// TODO(ws): Handle the case with multiple ResolvedIds.
			placeToDCID[entity.GetNode()] = entity.GetResolvedIds()[0]
		}
	}

	return placeToDCID, nil
}

func writeOutput(
	observations []*observation,
	triples []*triple,
	outputDir string,
) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	// Observations.
	fObservations, err := os.Create(filepath.Join(outputDir, "observations.csv"))
	if err != nil {
		return err
	}
	defer fObservations.Close()
	wObservations := csv.NewWriter(fObservations)
	if err := wObservations.Write(observationsHeader); err != nil {
		return err
	}
	for _, o := range observations {
		if err := wObservations.Write(
			[]string{o.entity, o.variable, o.date, o.value}); err != nil {
			return err
		}
	}
	wObservations.Flush()

	// Triples.
	fTriples, err := os.Create(filepath.Join(outputDir, "triples.csv"))
	if err != nil {
		return err
	}
	defer fTriples.Close()
	wTriples := csv.NewWriter(fTriples)
	if err := wTriples.Write(triplesHeader); err != nil {
		return err
	}
	for _, t := range triples {
		if err := wTriples.Write(
			[]string{t.subjectID, t.predicate, t.objectID, t.objectValue}); err != nil {
			return err
		}
	}
	wTriples.Flush()

	return nil
}
