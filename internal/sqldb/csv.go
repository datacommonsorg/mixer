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

package sqldb

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type csvHandle struct {
	f     io.Reader
	name  string
	close func()
}

type observation struct {
	entity     string
	variable   string
	date       string
	value      string
	provenance string
}

type triple struct {
	subjectID   string
	predicate   string
	objectID    string
	objectValue string
}

// Get csv file handle for observation csv and triples csv.
// Make sure to close the file returned from this function.
func listCSVFiles(dir string) ([]*csvHandle, []*csvHandle, error) {
	var obsCSVs []*csvHandle
	var tripleCSVs []*csvHandle
	if bucketName, objectPrefix, ok := parseGCSPath(dir); ok {
		// Read from GCS
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, nil, err
		}
		defer client.Close()
		bucket := client.Bucket(bucketName)
		query := &storage.Query{
			Prefix: objectPrefix,
		}
		it := bucket.Objects(ctx, query)
		for {
			objAttrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, nil, err
			}
			// Check if it's a CSV file
			if strings.HasSuffix(objAttrs.Name, ".csv") {
				rc, err := bucket.Object(objAttrs.Name).NewReader(ctx)
				if err != nil {
					return nil, nil, err
				}
				parts := strings.Split(objAttrs.Name, "/")
				fileName := parts[len(parts)-1]
				if strings.HasPrefix(fileName, "observations") {
					obsCSVs = append(
						obsCSVs,
						&csvHandle{
							f:     rc,
							name:  objAttrs.Name,
							close: func() { rc.Close() },
						},
					)
				} else if strings.HasPrefix(fileName, "triples") {
					tripleCSVs = append(
						tripleCSVs,
						&csvHandle{
							f:     rc,
							name:  objAttrs.Name,
							close: func() { rc.Close() },
						},
					)
				}
			}
		}
	} else {
		// Read from local files
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, nil, err
		}
		for _, file := range files {
			if fName := file.Name(); strings.HasSuffix(fName, ".csv") {
				f, err := os.Open(filepath.Join(dir, fName))
				if err != nil {
					return nil, nil, err
				}
				if strings.HasPrefix(fName, "observations") {
					obsCSVs = append(
						obsCSVs,
						&csvHandle{
							f:     f,
							name:  fName,
							close: func() { f.Close() },
						},
					)
				} else if strings.HasPrefix(fName, "triples") {
					tripleCSVs = append(
						tripleCSVs,
						&csvHandle{
							f:     f,
							name:  fName,
							close: func() { f.Close() },
						},
					)
				}
			}
		}
	}
	return obsCSVs, tripleCSVs, nil
}

func processObservationCSV(
	medatata *resource.Metadata,
	ch *csvHandle,
	provID string,
) (
	[]*observation,
	error,
) {
	records, err := csv.NewReader(ch.f).ReadAll()
	if err != nil {
		return nil, err
	}
	numRecords := len(records)
	if numRecords < 2 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"Empty CSV file %s", provID)
	}

	// Load header.
	header := records[0]
	if len(header) < 3 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"Less than 3 columns in CSV file %s", provID)
	}
	numColumns := len(header)

	// Generate observations.
	observations := []*observation{}
	for i := 1; i < numRecords; i++ {
		record := records[i]
		for j := 2; j < numColumns; j++ {
			if record[j] == "" {
				log.Printf("Skip empty value record: %s", record)
				continue
			}
			observations = append(observations, &observation{
				entity:     record[0],
				date:       record[1],
				variable:   header[j],
				value:      record[j],
				provenance: provID,
			})
		}
	}
	return observations, nil
}

func processTripleCSV(
	medatata *resource.Metadata,
	ch *csvHandle,
	provID string,
) (
	[]*triple,
	error,
) {
	records, err := csv.NewReader(ch.f).ReadAll()
	if err != nil {
		return nil, err
	}
	numRecords := len(records)
	if numRecords < 2 {
		return nil, status.Errorf(codes.FailedPrecondition, "Empty triple CSV file %s", provID)
	}
	header := records[0]
	if len(header) != 4 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"should have 4 columns in Triple CSV file %s", provID)
	}
	triples := []*triple{}
	for i := 1; i < numRecords; i++ {
		record := records[i]
		triples = append(triples, &triple{
			subjectID:   record[0],
			predicate:   record[1],
			objectID:    record[2],
			objectValue: record[3],
		})
	}
	return triples, nil
}
