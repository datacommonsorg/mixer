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
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/datacommonsorg/mixer/internal/server/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const batchSize = 1000

// Write writes raw CSV files to SQLite CSV files.
func Write(sqlClient *sql.DB, resourceMetadata *resource.Metadata) error {
	fileDir := resourceMetadata.SQLDataPath
	obsFiles, tripleFiles, err := listCSVFiles(fileDir)
	if err != nil {
		return err
	}
	if len(obsFiles) == 0 || len(tripleFiles) == 0 {
		return status.Errorf(codes.FailedPrecondition, "No CSV files found in %s", fileDir)
	}
	for _, csvFile := range obsFiles {
		provID := fmt.Sprintf("dc/custom/%s", strings.TrimRight(csvFile.name, ".csv"))
		observations, err := processObservationCSV(resourceMetadata, csvFile, provID)
		csvFile.close()
		if err != nil {
			return err
		}
		log.Printf("Process: %s", csvFile.name)
		err = writeObservations(sqlClient, observations)
		if err != nil {
			return err
		}
	}
	for _, csvFile := range tripleFiles {
		provID := fmt.Sprintf("dc/custom/%s", strings.TrimRight(csvFile.name, ".csv"))
		triples, err := processTripleCSV(resourceMetadata, csvFile, provID)
		csvFile.close()
		if err != nil {
			return err
		}
		log.Printf("Process: %s", csvFile.name)
		err = writeTriples(sqlClient, triples)
		if err != nil {
			return err
		}
	}
	return nil
}

func batchInsert(
	sqlClient *sql.DB,
	tableName string,
	columnNames []string,
	values []interface{},
	batchSize int,
) error {
	valueStrings := make([]string, 0, batchSize)
	valueArgs := make([]interface{}, 0, batchSize*len(columnNames))

	placeholder := "(" + strings.TrimRight(strings.Repeat("?, ", len(columnNames)), ", ") + ")"
	for _, value := range values {
		valueStrings = append(valueStrings, placeholder)
		// Assuming each value is a slice of interfaces
		valueArgs = append(valueArgs, value.([]interface{})...)
		if len(valueStrings) >= batchSize {
			sqlStmt := fmt.Sprintf(
				`INSERT INTO %s(%s) VALUES %s`,
				tableName,
				strings.Join(columnNames, ","),
				strings.Join(valueStrings, ","),
			)
			_, err := sqlClient.Exec(sqlStmt, valueArgs...)
			if err != nil {
				return err
			}
			log.Printf("[INSERT] %s: %d entries", tableName, batchSize)
			valueStrings = valueStrings[:0] // reset slices for next batch
			valueArgs = valueArgs[:0]
		}
	}
	if len(valueStrings) > 0 {
		sqlStmt := fmt.Sprintf(
			`INSERT INTO %s(%s) VALUES %s`,
			tableName,
			strings.Join(columnNames, ","),
			strings.Join(valueStrings, ","),
		)
		_, err := sqlClient.Exec(sqlStmt, valueArgs...)
		if err != nil {
			return err
		}
		log.Printf("[INSERT] %s: %d entries", tableName, len(valueStrings))
	}
	return nil
}

func writeObservations(
	sqlClient *sql.DB,
	observations []*observation,
) error {
	values := make([]interface{}, len(observations))
	for i, o := range observations {
		values[i] = []interface{}{o.entity, o.variable, o.date, o.value, o.provenance}
	}
	return batchInsert(
		sqlClient,
		"observations",
		[]string{"entity", "variable", "date", "value", "provenance"},
		values,
		batchSize,
	)
}

func writeTriples(
	sqlClient *sql.DB,
	triples []*triple,
) error {
	values := make([]interface{}, len(triples))
	for i, t := range triples {
		values[i] = []interface{}{t.subjectID, t.predicate, t.objectID, t.objectValue}
	}
	return batchInsert(
		sqlClient,
		"triples",
		[]string{"subject_id", "predicate", "object_id", "object_value"},
		values,
		batchSize,
	)
}
