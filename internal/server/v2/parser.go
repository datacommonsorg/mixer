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

// Package v2 is the version 2 of the Data Commons REST API.
package v2

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	spaceNewLineReplacer  = strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "")
	squareBracketReplacer = strings.NewReplacer("[", "", "]", "")
)

type ContainedInPlace struct {
	Ancestor       string
	ChildPlaceType string
}

// splitWithDelim splits a graph expression by "->" and "<-"
func splitWithDelim(expr string, delim string) []string {
	res := []string{}
	parts := strings.Split(expr, delim)
	for i := 0; i < len(parts)-1; i++ {
		res = append(res, parts[i])
		res = append(res, delim)
	}
	res = append(res, parts[len(parts)-1])
	return res
}

func splitExpr(expr string) []string {
	replacer := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "")
	expr = replacer.Replace(expr)

	parts := splitWithDelim(expr, "->")
	res := []string{}
	for _, part := range parts {
		subParts := splitWithDelim(part, "<-")
		for _, sp := range subParts {
			if sp != "" {
				res = append(res, sp)
			}
		}
	}
	return res
}

// parseArc parses an Arc object
func parseArc(arrow, expr string) (*Arc, error) {
	arc := &Arc{}
	switch arrow {
	case "->":
		arc.Out = true
	case "<-":
		arc.Out = false
	default:
		return nil, status.Errorf(
			codes.InvalidArgument,
			"arc string should start with arrow but got %s",
			arrow,
		)
	}
	// No property defined; This is to fetch all the properties.
	if len(expr) == 0 {
		return arc, nil
	}

	// Remove space and new line.
	rawExpr := expr
	expr = spaceNewLineReplacer.Replace(expr)

	// [prop1, prop2]
	if expr[0] == '[' {
		if expr[len(expr)-1] != ']' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid list string: %s", rawExpr)
		}
		expr = expr[1 : len(expr)-1]

		bracketProps, bracketFilters, err := parseBracketList(expr)
		if err != nil {
			return nil, err
		}

		arc.BracketProps = bracketProps
		if len(bracketFilters) > 0 {
			arc.BracketFilters = bracketFilters
		}
		return arc, nil
	}
	for i := 0; i < len(expr); i++ {
		if expr[i] == '+' {
			// <-containedInPlace+
			arc.SingleProp = expr[0:i]
			arc.Decorator = "+"
			expr = expr[i+1:]
			break
		}
		if expr[i] == '{' {
			// <-containedInPlace{p:v}
			arc.SingleProp = expr[0:i]
			if arc.SingleProp == "" {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid property expression: %s", rawExpr)
			}
			expr = expr[i:]
			break
		}
	}
	// {prop1:[val1_1, val1_2], prop2:val2}
	if len(expr) > 0 && expr[0] == '{' {
		filter, err := parseFilterString(expr)
		if err != nil {
			return nil, err
		}
		arc.Filter = filter
		return arc, nil
	}
	// No '+' or '{' found, this is a single property.
	if len(expr) > 0 {
		arc.SingleProp = expr
	}
	return arc, nil
}

// ParseProperty parses an expression string into a list of Arcs.
func ParseProperty(expr string) ([]*Arc, error) {
	parts := splitExpr(expr)
	if len(parts) == 1 {
		// Handle "->" query, which is to get all properties
		parts = append(parts, "")
	}
	if len(parts)%2 == 1 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid expression string: %s", expr)
	}
	arcs := []*Arc{}
	for i := 0; i < len(parts)/2; i++ {
		arc, err := parseArc(parts[i*2], parts[i*2+1])
		if err != nil {
			return nil, err
		}
		arcs = append(arcs, arc)
	}
	return arcs, nil
}

// ParseLinkedNodes parses an expression string into linked nodes.
func ParseLinkedNodes(expr string) (*LinkedNodes, error) {
	parts := splitExpr(expr)
	if len(parts) < 3 || len(parts)%2 == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid expression string: %s", expr)
	}
	g := &LinkedNodes{
		Subject: parts[0],
	}
	for i := 0; i < len(parts)/2; i++ {
		arc, err := parseArc(parts[i*2+1], parts[i*2+2])
		if err != nil {
			return nil, err
		}
		g.Arcs = append(g.Arcs, arc)
	}
	return g, nil
}

// Gets the contained in place from an expression.
func ParseContainedInPlace(expr string) (*ContainedInPlace, error) {
	g, err := ParseLinkedNodes(expr)
	if err != nil {
		return nil, err
	}
	if len(g.Arcs) != 1 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid expression string: %s", expr)
	}
	arc := g.Arcs[0]
	typeOfs, typeOfsOK := arc.Filter["typeOf"]
	if arc.SingleProp != ContainedInPlaceProperty ||
		arc.Decorator != "+" ||
		arc.Filter == nil ||
		!typeOfsOK || len(typeOfs) != 1 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid expression string: %s", expr)
	}
	if len(typeOfs) < 1 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid expression string: %s", expr)
	}
	return &ContainedInPlace{Ancestor: g.Subject, ChildPlaceType: typeOfs[0]}, nil
}

// parseBracketList processes the inner contents of a bracketed property list
func parseBracketList(expr string) ([]string, map[string]map[string][]string, error) {
	var bracketProps []string
	bracketFilters := make(map[string]map[string][]string)

	start := 0
	inBraces := false

	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '{':
			inBraces = true
		case '}':
			inBraces = false
		case ',':
			if !inBraces {
				part := expr[start:i]
				if part != "" {
					prop, filter, err := extractPropAndFilter(part)
					if err != nil {
						return nil, nil, err
					}
					bracketProps = append(bracketProps, prop)
					if filter != nil {
						bracketFilters[prop] = filter
					}
				}
				start = i + 1
			}
		}
	}

	// Handle the final element after the last comma
	if start < len(expr) {
		part := expr[start:]
		if part != "" {
			prop, filter, err := extractPropAndFilter(part)
			if err != nil {
				return nil, nil, err
			}
			bracketProps = append(bracketProps, prop)
			if filter != nil {
				bracketFilters[prop] = filter
			}
		}
	}

	return bracketProps, bracketFilters, nil
}

// extractPropAndFilter separates a property name from its optional inline filter.
func extractPropAndFilter(part string) (string, map[string][]string, error) {
	idx := strings.IndexByte(part, '{')
	if idx == -1 {
		return part, nil, nil
	}

	prop := part[:idx]
	if prop == "" {
		return "", nil, status.Errorf(
			codes.InvalidArgument, "invalid property expression: %s", part)
	}

	filter, err := parseFilterString(part[idx:])
	if err != nil {
		return "", nil, err
	}
	return prop, filter, nil
}

func parseFilterString(expr string) (map[string][]string, error) {
	if expr[0] != '{' || expr[len(expr)-1] != '}' {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid filter string: %s", expr)
	}
	filter := map[string][]string{}
	inner := squareBracketReplacer.Replace(expr[1 : len(expr)-1])
	parts := strings.Split(inner, ",")
	lastKey := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			kv := strings.Split(part, ":")
			if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", expr)
			}
			lastKey = kv[0]
			filter[lastKey] = append(filter[lastKey], kv[1])
		} else { // No ":" means this is another val in square bracket.
			if lastKey == "" {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", expr)
			}
			filter[lastKey] = append(filter[lastKey], part)
		}
	}
	return filter, nil
}
