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

	_ "github.com/mattn/go-sqlite3" // sqlite3 used for the search database.

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

const defaultLimit = 5

// Table and triggers creation:
// - 'statvars' is the main table containing information about a single statistical variable.
// - 'statvars_fts_idx' is a virtual FTS5 table used for providing full-text search capabilities on the statvars' content field.
// The setup follow the "External Content Tables" in the FTS5 documentation: https://www.sqlite.org/fts5.html
const sqlCreateTables = `
CREATE TABLE statvars(
	doc_id INTEGER PRIMARY KEY,
	dcid TEXT,
	name TEXT,
	signature INTEGER UNIQUE NOT NULL,
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

// See "The 'optimize' Command" in https://www.sqlite.org/fts5.html
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

// Returns the highest scoring result.
// In case of ties, returns statvars in descending order by number of constraints.
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

// Exact search by signature.
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

// A Document models a statvar indexed in the sqlite DB.
type Document struct {
	// ID of the statvar.
	ID string
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

func buildSortedDocumentSet(rawSvg map[string]*pb.StatVarGroupNode) []Document {
	documents := make([]Document, 0)
	for _, svgData := range rawSvg {
		for _, svData := range svgData.ChildStatVars {
			if strings.HasPrefix(svData.Id, "dc/") {
				continue
			}
			numConstraints := int32(strings.Count(svData.Definition, ",") + 1)
			keyValueText := strings.Replace(strings.Replace(svData.Definition, ",", " ", -1), "=", " ", -1)
			signature := computeSignature(strings.Split(keyValueText, " "))

			documents = append(documents, Document{
				ID:             svData.Id,
				Title:          svData.DisplayName,
				Signature:      signature,
				KeyValueText:   keyValueText,
				NumConstraints: numConstraints,
			})
		}
	}
	sort.Slice(documents, func(i, j int) bool {
		return documents[i].ID < documents[j].ID
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
		_, err = stmt.Exec(index, doc.ID, doc.Title, doc.Signature, doc.NumConstraints, doc.KeyValueText)
		if err != nil {
			// fmt.Printf("Ignoring statvar with DCID=%s as its signature is not unique.\n", doc.ID)
			continue
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

func parseSQLQueryResults(err error, rows *sql.Rows, result *pb.GetStatVarMatchResponse) error {
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
	return parseSQLQueryResults(err, rows, result)
}

func searchBySignature(db *sql.DB, queryTerms []string, result *pb.GetStatVarMatchResponse) error {
	rows, err := db.Query(sqlSearchBySignature, computeSignature(queryTerms))
	return parseSQLQueryResults(err, rows, result)
}

func escapeForSqliteSearch(terms []string) []string {
	escapedTerms := make([]string, 0)
	for _, token := range terms {
		jsonToken, _ := json.Marshal(token)
		escapedTerms = append(escapedTerms, string(jsonToken))
	}
	return escapedTerms
}

func searchRelatedStatvars(db *sql.DB, queryTerms []string, limit int32, result *pb.GetStatVarMatchResponse) error {
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
	err := searchRelatedStatvars(cache.SQLiteDb, queryTerms, limit, result)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(result.MatchInfo, func(i, j int) bool {
		return result.MatchInfo[i].Score > result.MatchInfo[j].Score
	})
	return result, nil
}
