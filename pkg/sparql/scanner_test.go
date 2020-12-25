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

package sparql

import (
	"reflect"
	"strings"
	"testing"
)

// Ensure the scanner can scan tokens correctly.
func TestScanSimple(t *testing.T) {
	var tests = []struct {
		s   string
		tok Token
		lit string
		pos Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: EOF},
		{s: `#`, tok: HASH, lit: "#"},
		{s: ` `, tok: WS, lit: " "},
		{s: "\t", tok: WS, lit: "\t"},
		{s: "\n", tok: WS, lit: "\n"},
		{s: "\r", tok: WS, lit: "\n"},
		{s: "\r\n", tok: WS, lit: "\n"},
		{s: "\rX", tok: WS, lit: "\n"},
		{s: "\n\r", tok: WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: WS, lit: " \n\t \n\t"},
		{s: " foo", tok: WS, lit: " "},

		// Logical operators
		{s: `AND`, tok: AND},
		{s: `and`, tok: AND},
		{s: `OR`, tok: OR},
		{s: `or`, tok: OR},

		{s: `=`, tok: EQ},
		{s: `! `, tok: ILLEGAL, lit: "!"},

		// Misc tokens
		{s: `(`, tok: LPAREN},
		{s: `)`, tok: RPAREN},
		{s: `,`, tok: COMMA},
		{s: `;`, tok: SEMICOLON},
		{s: `.`, tok: DOT, lit: "."},

		// Identifiers
		{s: `foo`, tok: IDENT, lit: `foo`},
		{s: `_foo`, tok: IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: IDENT, lit: `Zx12_3U_`},
		{s: `test"`, tok: BADSTRING, lit: "\"", pos: Pos{Line: 0, Char: 4}},
		{s: `"test`, tok: BADSTRING, lit: `test`},
		{s: `?host`, tok: VARIABLE, lit: `?host`},
		{s: `geoId/06`, tok: IDENT, lit: `geoId/06`},

		{s: `true`, tok: TRUE},
		{s: `false`, tok: FALSE},

		// Strings
		{s: `'testing 123!'`, tok: STRING, lit: `testing 123!`},
		{s: `"foo"`, tok: STRING, lit: `foo`},
		{s: `"foo\\bar"`, tok: STRING, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: BADESCAPE, lit: `\b`, pos: Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: STRING, lit: `foo"bar"`},
		{s: `'foo\nbar'`, tok: STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: STRING, lit: "foo\\bar"},
		{s: `'test`, tok: BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: BADESCAPE, lit: `\g`, pos: Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: NUMBER, lit: `100`},
		{s: `100.23`, tok: NUMBER, lit: `100.23`},
		{s: `.23`, tok: NUMBER, lit: `.23`},
		{s: `10.3s`, tok: NUMBER, lit: `10.3`},

		// Keywords
		{s: `BASE`, tok: BASE},
		{s: `BY`, tok: BY},
		{s: `FILTER`, tok: FILTER},
		{s: `FROM`, tok: FROM},
		{s: `IN`, tok: IN},
		{s: `LIMIT`, tok: LIMIT},
		{s: `ORDER`, tok: ORDER},
		{s: `PREFIX`, tok: PREFIX},
		{s: `SELECT`, tok: SELECT},
		{s: `WHERE`, tok: WHERE},
		{s: `seLECT`, tok: SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

func TestMulti(t *testing.T) {
	type result struct {
		tok Token
		pos Pos
		lit string
	}
	exp := []result{
		{tok: SELECT, pos: Pos{Line: 0, Char: 0}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 6}, lit: " "},
		{tok: VARIABLE, pos: Pos{Line: 0, Char: 7}, lit: "?name"},
		{tok: WS, pos: Pos{Line: 0, Char: 12}, lit: " "},
		{tok: WHERE, pos: Pos{Line: 0, Char: 13}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 18}, lit: " "},
		{tok: LBRAC, pos: Pos{Line: 0, Char: 19}, lit: ""},
		{tok: VARIABLE, pos: Pos{Line: 0, Char: 20}, lit: "?person"},
		{tok: WS, pos: Pos{Line: 0, Char: 27}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 28}, lit: "rdf:type"},
	}

	// Create a scanner.
	v := `SELECT ?name WHERE ` +
		`{?person rdf:type foaf:Person ; foaf:name ?name . ?person ex:age ?age} LIMIT 10`
	s := NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == EOF {
			break
		}
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Errorf("%d. token mismatch:\nexp=%#v\ngot=%#v\n\n", i, exp[i], act[i])
		}
	}
}
