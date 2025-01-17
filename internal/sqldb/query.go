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
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// Requests for latest dates include this literal for date in the request.
	latestDate = "LATEST"
	// Key for SV groups in the key_value_store table.
	StatVarGroupsKey = "StatVarGroups"
	// Chunk size for CTE (Common Table Expression) statements.
	// Chunking avoids issues where certain dbs (like sqlite) can't handle a large number of items in a CTE.
	cteChunkSize = 500
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

// GetObservationCount returns number of observations by entity and variable.
func (sc *SQLClient) GetObservationCount(ctx context.Context, variables []string, entities []string) ([]*ObservationCount, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetObservationCount")
	var observations []*ObservationCount
	if len(variables) == 0 || len(entities) == 0 {
		return observations, nil
	}

	entitiesStmt := generateCTESelectStatement("entity", entities)
	variablesStmt := generateCTESelectStatement("variable", variables)

	stmt := statement{
		query: fmt.Sprintf(statements.getObsCountByVariableAndEntity, entitiesStmt.query, variablesStmt.query),
		args:  util.MergeMaps(entitiesStmt.args, variablesStmt.args),
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

// GetEntityInfoTriples returns name and typeOf triples for the specified entities.
func (sc *SQLClient) GetEntityInfoTriples(ctx context.Context, entities []string) ([]*Triple, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetEntityInfoTriples")
	var rows []*Triple
	if len(entities) == 0 {
		return rows, nil
	}

	stmt := statement{
		query: statements.getEntityInfoTriples,
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
func (sc *SQLClient) GetNodeTriples(ctx context.Context, nodes []string, properties []string, direction string) ([]*Triple, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetNodeTriples")
	// Some requests result in querying the DB for O(K) nodes which causes issues with the CTE (Common Table Expression) statements in some dbs.
	// So we query for them in chunks and collate the resulting triples.
	nodeChunks := chunkSlice(nodes, cteChunkSize)

	errGroup, errCtx := errgroup.WithContext(ctx)
	triplesChans := []chan []*Triple{}
	for _, chunk := range nodeChunks {
		// Assign to a local variable so it can be used in go routines.
		chunk := chunk
		ch := make(chan []*Triple, 1)
		triplesChans = append(triplesChans, ch)
		errGroup.Go(func() error {
			defer close(ch)
			triplesChunk, err := sc.getNodeChunkTriples(errCtx, chunk, properties, direction)
			if err != nil {
				return err
			}
			ch <- triplesChunk
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	triples := []*Triple{}
	for _, ch := range triplesChans {
		triples = append(triples, <-ch...)
	}

	return triples, nil
}

// getNodeChunkTriples retrieves triples from SQL for the specified node chunk and properties in the specified direction (in or out).
func (sc *SQLClient) getNodeChunkTriples(ctx context.Context, nodeChunk []string, properties []string, direction string) ([]*Triple, error) {
	defer util.TimeTrack(time.Now(), "SQL: getNodeChunkTriples")
	var rows []*Triple
	if len(nodeChunk) == 0 || len(properties) == 0 {
		return rows, nil
	}

	nodesStmt := generateCTESelectStatement("node", nodeChunk)
	propsStmt := generateCTESelectStatement("prop", properties)

	var stmt statement

	switch direction {
	case util.DirectionOut:
		stmt = statement{
			query: fmt.Sprintf(statements.getSubjectTriples, nodesStmt.query, propsStmt.query),
			args:  util.MergeMaps(nodesStmt.args, propsStmt.args),
		}
	default:
		stmt = statement{
			query: fmt.Sprintf(statements.getObjectTriples, nodesStmt.query, propsStmt.query),
			args:  util.MergeMaps(nodesStmt.args, propsStmt.args),
		}
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
	query := fmt.Sprintf(statements.getTableColumns, tableName)

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

// generateCTESelectStatement generates a select statement for a CTE (Common Table Expression).
// e.g. for a CTE of the form "WITH node_list(nodes) AS (SELECT 'node1' UNION ALL 'node2')",
// it returns the SELECT portion of the CTE parameterized as a statement object.
func generateCTESelectStatement(paramPrefix string, values []string) statement {
	var sb strings.Builder
	args := map[string]interface{}{}

	for i, value := range values {
		param := fmt.Sprintf("%s%d", paramPrefix, i+1)
		args[param] = value
		sb.WriteString(fmt.Sprintf("SELECT :%s", param))
		if i < len(values)-1 {
			sb.WriteString(" UNION ALL ")
		}
	}

	return statement{
		query: sb.String(),
		args:  args,
	}
}

func chunkSlice(items []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize

		if end > len(items) {
			end = len(items)
		}

		chunks = append(chunks, items[i:end])
	}

	return chunks
}

// statement struct includes the sql query and named args used to execute a sql query.
type statement struct {
	query string
	args  map[string]interface{}
}
