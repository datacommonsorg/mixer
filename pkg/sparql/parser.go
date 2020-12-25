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
	"fmt"
	"io"
	"strconv"
)

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// QueryTree represents a parsed Sparql syntax tree.
type QueryTree struct {
	P *Prologue
	S *Select
	W *Where
	O *Orderby
	L int
}

// Prologue represents query prologue information
type Prologue struct {
	Base   string
	Prefix map[string]string
}

// Select contains information in the SELECT statement.
type Select struct {
	Variable []string
	Distinct bool
}

// Triple reprensts a triple in Sparql query.
type Triple struct {
	Sub  string
	Pred string
	Objs []string
}

// Where represents the where condition in Sparql query.
type Where struct {
	Triples []Triple
}

// Orderby represents the order by condition.
type Orderby struct {
	Variable string
	ASC      bool
}

// Parser represents a Sparql parser.
type Parser struct {
	s *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

func (p *Parser) parseURI() string {
	uri := ""
	for {
		tok, _, lit := p.ScanIgnoreWhitespace()
		if tok == EOF {
			break
		}
		uri += lit
		if tok == GT {
			break
		}
	}
	return uri
}

func (p *Parser) parsePrologue() (*Prologue, *ParseError) {
	result := Prologue{Prefix: map[string]string{}}
	for {
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == BASE {
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != LT {
				return nil, newParseError(tokstr(tok, lit), []string{"<"}, pos)
			}
			p.Unscan()
			result.Base = p.parseURI()
		} else if tok == PREFIX {
			_, _, pre := p.ScanIgnoreWhitespace()
			if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != LT {
				return nil, newParseError(tokstr(tok, lit), []string{"<"}, pos)
			}
			p.Unscan()
			result.Prefix[pre] = p.parseURI()
		} else {
			p.Unscan()
			return &result, nil
		}
	}
}

func (p *Parser) parseSelect() (*Select, *ParseError) {
	result := Select{}
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != SELECT {
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
	}
	tok, _, _ = p.ScanIgnoreWhitespace()
	if tok == DISTINCT {
		result.Distinct = true
	} else {
		p.Unscan()
	}
	for {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		if tok == FROM || tok == WHERE || tok == EOF {
			p.Unscan()
			return &result, nil
		}
		if tok != VARIABLE {
			return nil, newParseError(tokstr(tok, lit), []string{"?..."}, pos)
		}
		result.Variable = append(result.Variable, lit)
	}
}

func (p *Parser) parseWhere() (*Where, *ParseError) {
	result := Where{}
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != WHERE {
		return nil, newParseError(tokstr(tok, lit), []string{"Where"}, pos)
	}
	tok, pos, lit = p.ScanIgnoreWhitespace()
	if tok != LBRAC {
		return nil, newParseError(tokstr(tok, lit), []string{"{"}, pos)
	}
	var sub string
	var pred string
	var objs []string
	idx := 0
	for {
		tok, _, lit := p.ScanIgnoreWhitespace()
		if tok == EOF {
			return nil, newParseError(tokstr(tok, lit), []string{"}"}, pos)
		}
		if tok == RBRAC {
			if sub != "" && pred != "" {
				result.Triples = append(result.Triples, Triple{sub, pred, objs})
			}
			return &result, nil
		}
		if tok == DOT {
			result.Triples = append(result.Triples, Triple{sub, pred, objs})
			idx = 0
			sub = ""
			pred = ""
			objs = []string{}
			continue
		}
		if tok == LPAREN || tok == RPAREN {
			continue
		}
		switch idx {
		case 0:
			sub = lit
			idx++
		case 1:
			pred = lit
			idx++
		case 2:
			if tok == STRING {
				lit = fmt.Sprintf(`"%s"`, lit)
			}
			objs = append(objs, lit)
		}
	}
}

func (p *Parser) parseOrderBy() (*Orderby, *ParseError) {
	varString := ""
	asc := true
	tok, _, _ := p.ScanIgnoreWhitespace()
	if tok == EOF {
		return nil, nil
	}
	if tok != ORDER {
		p.Unscan()
		return nil, nil
	}
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}
	tok, pos, lit = p.ScanIgnoreWhitespace()
	if tok == ASC || tok == DESC {
		asc = tok == ASC
		tok, pos, lit = p.ScanIgnoreWhitespace()
		if tok != LPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{"("}, pos)
		}
		tok, pos, lit = p.ScanIgnoreWhitespace()
		if tok != VARIABLE {
			return nil, newParseError(tokstr(tok, lit), []string{"?..."}, pos)
		}
		varString = lit
		tok, pos, lit = p.ScanIgnoreWhitespace()
		if tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}
	} else {
		if tok != VARIABLE {
			return nil, newParseError(tokstr(tok, lit), []string{"?..."}, pos)
		}
		varString = lit
	}
	return &Orderby{varString, asc}, nil
}

func (p *Parser) parseLimit() (int, *ParseError) {
	limit := 0
	tok, pos, lit := p.ScanIgnoreWhitespace()
	if tok == EOF {
		return 0, nil
	}
	if tok != LIMIT {
		return 0, newParseError(tokstr(tok, lit), []string{"LIMIT"}, pos)
	}
	tok, pos, lit = p.ScanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"NUMBER"}, pos)
	}
	limit, err := strconv.Atoi(lit)
	if err != nil {
		return 0, newParseError(tokstr(tok, lit), []string{"NUMBER"}, pos)
	}
	return limit, nil
}

// Parse parses sparql query into syntax tree.
func (p *Parser) Parse() (*QueryTree, *ParseError) {
	prologue, err := p.parsePrologue()
	if err != nil {
		return nil, err
	}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	where, err := p.parseWhere()
	if err != nil {
		return nil, err
	}
	orderby, err := p.parseOrderBy()
	if err != nil {
		return nil, err
	}
	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}
	return &QueryTree{P: prologue, S: sel, W: where, O: orderby, L: limit}, nil
}

// Scan returns the next token from the underlying scanner.
func (p *Parser) Scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// Unscan pushes the previously read token back onto the buffer.
func (p *Parser) Unscan() { p.s.Unscan() }

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token.
func (p *Parser) ScanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	for {
		tok, pos, lit = p.Scan()
		if tok == WS {
			continue
		}
		return
	}
}
