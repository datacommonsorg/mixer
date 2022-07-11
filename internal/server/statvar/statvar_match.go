// Copyright 2022 Google LLC
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

package statvar

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

const defaultLimit = 5

const sqlCreateTables = `
CREATE TABLE statvars(
	doc_id INTEGER PRIMARY KEY,
	dcid TEXT,
	name TEXT,
	signature INTEGER,
	num_constraints INTEGER,
	content TEXT);

CREATE VIRTUAL TABLE statvars_fts_idx USING fts5(
	content,
	content="statvars",
	content_rowid="doc_id",
	tokenize = "ascii tokenchars '-_'");

-- Triggers to keep the FTS index up to date.
CREATE TRIGGER statvars_ai AFTER INSERT ON statvars
BEGIN
	INSERT INTO statvars_fts_idx(rowid, content)
	VALUES (new.doc_id, new.content);
END;
CREATE TRIGGER statvars_ad AFTER DELETE ON statvars
BEGIN
	INSERT INTO statvars_fts_idx(statvars_fts_idx, rowid, content)
	VALUES ('delete', old.doc_id, old.content);
END;
CREATE TRIGGER statvars_au AFTER UPDATE ON statvars
BEGIN
	INSERT INTO statvars_fts_idx(statvars_fts_idx, rowid, content)
	VALUES ('delete', old.doc_id, old.content);

	INSERT INTO statvars_fts_idx(rowid, content)
	VALUES (new.doc_id, new.content);
END;
`

const sqlOptimize = `INSERT INTO statvars_fts_idx(content) VALUES('optimize');`

const sqlInsert = `
INSERT INTO statvars(
	doc_id,
	dcid,
	name,
	signature,
	num_constraints,
	content) VALUES (?, ?, ?, ?, ?, ?);
`

const sqlSearch = `
WITH statvars_rows AS (
	SELECT
		ROWID,
		*
	FROM
		statvars
)
SELECT
	statvars_fts_idx.rank * -1 as rank,
	statvars_rows.dcid AS dcid,
	statvars_rows.name AS name,
	"%s" || highlight(statvars_fts_idx, 0, '[', ']') AS explanation
FROM
	statvars_rows
	JOIN statvars_fts_idx ON statvars_rows.ROWID = statvars_fts_idx.ROWID
WHERE
	statvars_fts_idx MATCH (?)
ORDER BY
	statvars_fts_idx.rank,
	statvars_rows.num_constraints DESC
LIMIT
	%d;
`

const sqlSearchBySignature = `
SELECT
	1.0 AS rank,
	dcid,
	name,
	"Full match on: " || content AS explanation
FROM
	statvars
WHERE
	signature = (?)
LIMIT
	1;
`

// A StatVarDocument models a document indexed by the sqlite db.
type StatVarDocument struct {
	// Id of the statvar.
	Id string
	// Title of the document. For a statvar this will be the DisplayName.
	Title string
	// A signature text that uniquely identifies the statvar from the key value text.
	Signature int
	// A key value pairs string describing the properties of a stat var.
	KeyValueText string
	// Number of constraints associated to a statvar.
	NumConstraints int32
}

func computeSignature(terms []string) int {
	sort.Strings(terms)
	hasher := fnv.New32()
	for _, term := range terms {
		_, err := hasher.Write([]byte(term))
		// Ignore the error.
		if err != nil {
			continue
		}
	}
	return int(hasher.Sum32())
}

func buildSortedDocumentSet(rawSvg map[string]*pb.StatVarGroupNode) []StatVarDocument {
	documents := make([]StatVarDocument, 0)
	for _, svgData := range rawSvg {
		for _, svData := range svgData.ChildStatVars {
			numConstraints := int32(strings.Count(svData.Definition, ",") + 1)
			keyValueText := strings.Replace(strings.Replace(svData.Definition, ",", " ", -1), "=", " ", -1)
			signature := computeSignature(strings.Split(keyValueText, " "))

			documents = append(documents, StatVarDocument{
				Id:             svData.Id,
				Title:          svData.DisplayName,
				Signature:      signature,
				KeyValueText:   keyValueText,
				NumConstraints: numConstraints,
			})
		}
	}
	sort.Slice(documents, func(i, j int) bool {
		return documents[i].Id < documents[j].Id
	})
	return documents
}

// BuildSQLiteIndex builds the sqlite search index for all the stat vars.
func BuildSQLiteIndex(
	rawSvg map[string]*pb.StatVarGroupNode,
) (*sql.DB, error) {
	defer util.TimeTrack(time.Now(), "BuildSQLiteIndex")
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(sqlCreateTables)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare(sqlInsert)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	docSet := buildSortedDocumentSet(rawSvg)
	for index, doc := range docSet {
		_, err = stmt.Exec(index, doc.Id, doc.Title, doc.Signature, doc.NumConstraints, doc.KeyValueText)
		if err != nil {
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(sqlOptimize)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func parseSqlQueryResults(err error, rows *sql.Rows, result *pb.GetStatVarMatchResponse) error {
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var score float64
		matchInfo := &pb.GetStatVarMatchResponse_MatchInfo{}
		err := rows.Scan(&score, &matchInfo.StatVar, &matchInfo.StatVarName, &matchInfo.Explanation)
		if err != nil {
			return err
		}
		matchInfo.Score = roundFloat(score, 4)
		result.MatchInfo = append(result.MatchInfo, matchInfo)
	}
	return nil
}

func searchByTerms(db *sql.DB, escapedQuery string, explanationPrefix string, limit int32, result *pb.GetStatVarMatchResponse) error {
	rows, err := db.Query(fmt.Sprintf(sqlSearch, explanationPrefix, limit), escapedQuery)
	return parseSqlQueryResults(err, rows, result)
}

func searchBySignature(db *sql.DB, queryTerms []string, result *pb.GetStatVarMatchResponse) error {
	rows, err := db.Query(sqlSearchBySignature, computeSignature(queryTerms))
	return parseSqlQueryResults(err, rows, result)
}

func escapeForSqliteSearch(terms []string) []string {
	escapedTerms := make([]string, 0)
	for _, token := range terms {
		jsonToken, _ := json.Marshal(token)
		escapedTerms = append(escapedTerms, string(jsonToken))
	}
	return escapedTerms
}

func SearchRelatedStatvars(db *sql.DB, queryTerms []string, limit int32, result *pb.GetStatVarMatchResponse) error {
	err := searchBySignature(db, queryTerms, result)
	if err != nil {
		return err
	}
	if len(result.MatchInfo) > 0 {
		return nil
	}
	escapedTerms := escapeForSqliteSearch(queryTerms)
	err = searchByTerms(db, strings.Join(escapedTerms, " "), "AND query: ", limit, result)
	if err != nil {
		return err
	}
	if len(result.MatchInfo) > 0 {
		return nil
	}
	return searchByTerms(db, strings.Join(escapedTerms, " OR "), "OR query: ", limit, result)
}

func normalizeToken(token string) string {
	if token == "measuredProperty" {
		return "mp"
	}
	if token == "populationType" {
		return "pt"
	}
	if token == "statType" {
		return "st"
	}
	if token == "measurementQualifier" {
		return "mq"
	}
	if token == "measurementDenominator" {
		return "md"
	}
	return token
}

func tokenizeQuery(query string) []string {
	tokens := make([]string, 0)
	for _, token := range strings.Split(query, " ") {
		tokens = append(tokens, normalizeToken(token))
	}
	return tokens
}

// GetStatVarMatch implements API for Mixer.GetStatVarMatch.
func GetStatVarMatch(
	ctx context.Context,
	in *pb.GetStatVarMatchRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.GetStatVarMatchResponse, error) {
	limit := in.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	queryTerms := tokenizeQuery(in.GetQuery())
	result := &pb.GetStatVarMatchResponse{}
	err := SearchRelatedStatvars(cache.SQLiteDb, queryTerms, limit, result)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(result.MatchInfo, func(i, j int) bool {
		return result.MatchInfo[i].Score > result.MatchInfo[j].Score
	})
	return result, nil
}
