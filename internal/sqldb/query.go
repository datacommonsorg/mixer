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

// GetSVSummaries retrieves summaries for the specified variables.
func (sc *SQLClient) GetSVSummaries(ctx context.Context, variables []string) ([]*SVSummary, error) {
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

// GetStatVarGroups retrieves all StatVarGroups from the database.
func (sc *SQLClient) GetStatVarGroups(ctx context.Context) ([]*StatVarGroup, error) {
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

// GetKeyValue gets the value for the specified key from the key_value_store table.
// If not found, returns false.
// If found, unmarshals the value into the specified proto and returns true.
func (sc *SQLClient) GetKeyValue(ctx context.Context, key string, out protoreflect.ProtoMessage) (bool, error) {
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

// statement struct includes the sql query and named args used to execute a sql query.
type statement struct {
	query string
	args  map[string]interface{}
}
