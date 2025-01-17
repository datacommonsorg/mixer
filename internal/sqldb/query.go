// Copyright 2025 Google LLC
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

// Queries executed by the SQLClient.
package sqldb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/jmoiron/sqlx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// Requests for latest dates include this literal for date in the request.
	latestDate = "LATEST"
	// Key for SV groups in the key_value_store table.
	StatVarGroupsKey = "StatVarGroups"
)

// GetObservations retrieves observations from SQL given a list of variables and entities and a date.
func (sc *SQLClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetObservations")
	var observations []*Observation
	if len(variables) == 0 || len(entities) == 0 {
		return observations, nil
	}

	var stmt statement

	switch {
	case date != "" && date != latestDate:
		stmt = statement{
			query: statements.getObsByVariableEntityAndDate,
			args: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
				"date":      date,
			},
		}
	default:
		stmt = statement{
			query: statements.getObsByVariableAndEntity,
			args: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
			},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&observations,
	)
	if err != nil {
		return nil, err
	}

	return observations, nil
}

// GetObservationsByEntityType retrieves observations from SQL given a list of variables and an entity type and a date.
func (sc *SQLClient) GetObservationsByEntityType(ctx context.Context, variables []string, entityType string, date string) ([]*Observation, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetObservationsByEntityType")

	var observations []*Observation
	if len(variables) == 0 {
		return observations, nil
	}

	var stmt statement

	switch {
	case date != "" && date != latestDate:
		stmt = statement{
			query: statements.getObsByVariableEntityTypeAndDate,
			args: map[string]interface{}{
				"variables":  variables,
				"entityType": entityType,
				"date":       date,
			},
		}
	default:
		stmt = statement{
			query: statements.getObsByVariableAndEntityType,
			args: map[string]interface{}{
				"variables":  variables,
				"entityType": entityType,
			},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&observations,
	)
	if err != nil {
		return nil, err
	}

	return observations, nil
}

// GetSVSummaries retrieves summaries for the specified variables.
func (sc *SQLClient) GetSVSummaries(ctx context.Context, variables []string) ([]*SVSummary, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetSVSummaries")
	var summaries []*SVSummary
	if len(variables) == 0 {
		return summaries, nil
	}

	stmt := statement{
		query: statements.getStatVarSummaries,
		args: map[string]interface{}{
			"variables": variables,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&summaries,
	)
	if err != nil {
		return nil, err
	}

	return summaries, nil
}

// GetAllStatVarGroups retrieves all StatVarGroups from the database.
func (sc *SQLClient) GetAllStatVarGroups(ctx context.Context) ([]*StatVarGroup, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetStatVarGroups")
	var svgs []*StatVarGroup

	stmt := statement{
		query: statements.getAllStatVarGroups,
		args:  map[string]interface{}{},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&svgs,
	)
	if err != nil {
		return nil, err
	}

	return svgs, nil
}

// GetAllStatisticalVariables retrieves all SVs from the database.
func (sc *SQLClient) GetAllStatisticalVariables(ctx context.Context) ([]*StatisticalVariable, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetAllStatisticalVariables")
	var svs []*StatisticalVariable

	stmt := statement{
		query: statements.getAllStatVars,
		args:  map[string]interface{}{},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&svs,
	)
	if err != nil {
		return nil, err
	}

	return svs, nil
}

// GetEntityCount returns number of entities (from given candidates) by variable, date and provenance.
func (sc *SQLClient) GetEntityCount(ctx context.Context, variables []string, entities []string) ([]*EntityCount, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetEntityCount")
	var counts []*EntityCount
	if len(variables) == 0 || len(entities) == 0 {
		return counts, nil
	}

	stmt := statement{
		query: statements.getEntityCountByVariableDateAndProvenance,
		args: map[string]interface{}{
			"variables": variables,
			"entities":  entities,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&counts,
	)
	if err != nil {
		return nil, err
	}

	return counts, nil
}

// GetNodePredicates retrieves (node, predicate) pairs from SQL for the specified entities in the specified direction (in or out).
func (sc *SQLClient) GetNodePredicates(ctx context.Context, entities []string, direction string) ([]*NodePredicate, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetNodePredicates")
	var observations []*NodePredicate
	if len(entities) == 0 {
		return observations, nil
	}

	var stmt statement

	switch direction {
	case util.DirectionOut:
		stmt = statement{
			query: statements.getSubjectPredicates,
			args: map[string]interface{}{
				"entities": entities,
			},
		}
	default:
		stmt = statement{
			query: statements.getObjectPredicates,
			args: map[string]interface{}{
				"entities": entities,
			},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&observations,
	)
	if err != nil {
		return nil, err
	}

	return observations, nil
}

// GetExistingStatVarGroups returns SVGs that exist in the SQL database from the specified group dcids.
func (sc *SQLClient) GetExistingStatVarGroups(ctx context.Context, groupDcids []string) ([]string, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetExistingStatVarGroups")

	values := []string{}

	// If no groups are specified, return an empty slice.
	if len(groupDcids) == 0 {
		return values, nil
	}

	stmt := statement{
		query: statements.getExistingStatVarGroups,
		args: map[string]interface{}{
			"groups": groupDcids,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&values,
	)
	if err != nil {
		return nil, err
	}
	return values, nil
}

// GetAllEntitiesOfType returns all entities of the specified type.
func (sc *SQLClient) GetAllEntitiesOfType(ctx context.Context, typeOf string) ([]*SubjectObject, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetAllEntitiesOfType")

	rows := []*SubjectObject{}

	stmt := statement{
		query: statements.getAllEntitiesOfType,
		args: map[string]interface{}{
			"type": typeOf,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&rows,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetContainedInPlace returns all entities of the specified childPlaceType that are contained in the specified parentPlaces.
func (sc *SQLClient) GetContainedInPlace(ctx context.Context, childPlaceType string, parentPlaces []string) ([]*SubjectObject, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetContainedInPlace")

	rows := []*SubjectObject{}

	if len(parentPlaces) == 0 {
		return rows, nil
	}

	stmt := statement{
		query: statements.getContainedInPlace,
		args: map[string]interface{}{
			"childPlaceType": childPlaceType,
			"parentPlaces":   parentPlaces,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&rows,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetEntityVariables returns variables associated with the specified entities.
func (sc *SQLClient) GetEntityVariables(ctx context.Context, entities []string) ([]*EntityVariables, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetEntityVariables")

	rows := []*EntityVariables{}

	if len(entities) == 0 {
		return rows, nil
	}

	stmt := statement{
		query: statements.getEntityVariables,
		args: map[string]interface{}{
			"entities": entities,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&rows,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetKeyValue gets the value for the specified key from the key_value_store table.
// If not found, returns false.
// If found, unmarshals the value into the specified proto and returns true.
func (sc *SQLClient) GetKeyValue(ctx context.Context, key string, out protoreflect.ProtoMessage) (bool, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetKeyValue")
	stmt := statement{
		query: statements.getKeyValue,
		args: map[string]interface{}{
			"key": key,
		},
	}

	values := []string{}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		&values,
	)
	if err != nil || len(values) == 0 {
		return false, err
	}

	bytes, err := util.UnzipAndDecode(values[0])
	if err != nil {
		return false, err
	}

	err = proto.Unmarshal(bytes, out)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (sc *SQLClient) queryAndCollect(
	ctx context.Context,
	stmt statement,
	dest interface{},
) error {
	// Convert named query and maps of args to placeholder query and list of args.
	query, args, err := sqlx.Named(stmt.query, stmt.args)
	if err != nil {
		return err
	}

	// Expand slice values.
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	// Transform query to the driver's placeholder type.
	query = sc.dbx.Rebind(query)

	return sc.dbx.SelectContext(ctx, dest, query, args...)
}

// ValidateDatabase checks if the SQL DB has all the tables and complies to the schema expected by the service.
// It returns an error if it does not.
func (sc *SQLClient) ValidateDatabase() error {
	err := sc.checkTables()
	if err != nil {
		return err
	}

	err = sc.checkSchema()
	if err != nil {
		return err
	}

	return nil
}

// checkTables checks if the SQL DB has all the tables expected by the service.
// It returns an error if it does not.
func (sc *SQLClient) checkTables() error {
	for _, tableName := range allTables {
		columnNames, err := sc.getTableColumns(tableName)
		// We use an error or empty column names as a signal that tables are not found.
		// We are not querying the db schema itself because those queries are different for different dbs.
		// This keeps it simple and agnostic of the underlying db.
		if err != nil || len(columnNames) == 0 {
			if err != nil {
				log.Printf("Error checking table %s: %v", tableName, err)
			}

			return fmt.Errorf(`The SQL database does not have the required tables.
The following tables are required: %s

Prepare and load your data before starting this service.
Guide: https://docs.datacommons.org/custom_dc/custom_data.html
			`, strings.Join(allTables, ", "))
		}
	}

	log.Printf("SQL tables check succeeded.")
	return nil
}

// checkSchema checks if the schema of the SQL DB is what is expected by the service.
// It returns an error if it is not.
func (sc *SQLClient) checkSchema() error {
	observationColumns, err := sc.getTableColumns(TableObservations)
	if err != nil {
		return err
	}

	missingObservationColumns := util.GetMissingStrings(observationColumns, allObservationsTableColumns)
	if len(missingObservationColumns) != 0 {
		return fmt.Errorf(`The following columns are missing in the %s table: %v

Run a data management job to update your database schema.
Guide: https://docs.datacommons.org/custom_dc/troubleshooting.html#schema-check-failed.

`,
			TableObservations, missingObservationColumns)
	}

	log.Printf("SQL schema check succeeded.")
	return nil
}

func (sc *SQLClient) getTableColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf(statements.getTableColumnsFormat, tableName)

	rows, err := sc.dbx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s (error: %w)", tableName, err)
	}
	defer rows.Close()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names for %s table: %w", tableName, err)
	}

	return columns, nil
}

// statement struct includes the sql query and named args used to execute a sql query.
type statement struct {
	query string
	args  map[string]interface{}
}
