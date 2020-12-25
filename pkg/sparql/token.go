// Copyright 2019 Google LLC
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

// Package sparql includes parsing utilities for Sparql query.
package sparql

import (
	"strings"
)

// Token is a lexical token in Sparql.
type Token int

const (
	// ILLEGAL and following are base tokens.
	ILLEGAL Token = iota
	EOF
	WS

	// IDENT and following are Sparql literal tokens.
	IDENT     // rdf
	URI       // <http://example.org>
	VARIABLE  // ?a
	NUMBER    // 123.45
	STRING    // "abc"
	BADSTRING // "abc
	BADESCAPE // \q
	TRUE      // true
	FALSE     // false

	// AND and following are Sparql operators.
	AND // AND
	OR  // OR
	EQ  // =

	LT        // <
	GT        // >
	LPAREN    // (
	RPAREN    // )
	LBRAC     // {
	RBRAC     // }
	COMMA     // ,
	SEMICOLON // ;
	DOT       //.
	HASH      // #

	keywordBeg
	// ASC and following are Sparql keywords.
	ASC
	BASE
	BY
	DESC
	DISTINCT
	FILTER
	FROM
	IN
	LIMIT
	ORDER
	PREFIX
	SELECT
	WHERE
	keywordEnd
)

var (
	tokens = [...]string{
		ILLEGAL: "ILLEGAL",
		EOF:     "EOF",
		WS:      "WS",

		IDENT:     "IDENT",
		URI:       "URI",
		VARIABLE:  "VARIABLE",
		NUMBER:    "NUMBER",
		STRING:    "STRING",
		BADSTRING: "BADSTRING",
		BADESCAPE: "BADESCAPE",
		TRUE:      "TRUE",
		FALSE:     "FALSE",

		AND: "AND",
		OR:  "OR",

		EQ: "=",

		LT:        "<",
		GT:        ">",
		LPAREN:    "(",
		RPAREN:    ")",
		LBRAC:     "{",
		RBRAC:     "}",
		COMMA:     ",",
		SEMICOLON: ";",
		DOT:       ".",
		HASH:      ".",

		ASC:      "ASC",
		BASE:     "BASE",
		BY:       "BY",
		DESC:     "DESC",
		DISTINCT: "DISTINCT",
		FILTER:   "FILTER",
		FROM:     "FROM",
		IN:       "IN",
		LIMIT:    "LIMIT",
		ORDER:    "ORDER",
		PREFIX:   "PREFIX",
		SELECT:   "SELECT",
		WHERE:    "WHERE",
	}

	keywords map[string]Token
)

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	}
	return 0
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}
