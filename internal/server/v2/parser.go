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
	if arrow == "->" {
		arc.Out = true
	} else if arrow == "<-" {
		arc.Out = false
	} else {
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
	replacer := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "")
	// [prop1, prop2]
	if expr[0] == '[' {
		if expr[len(expr)-1] != ']' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid list string: %s", expr)
		}
		expr = expr[1 : len(expr)-1]
		arc.BracketProps = strings.Split(replacer.Replace(expr), ",")
		return arc, nil
	}
	for i := 0; i < len(expr); i++ {
		if expr[i] == '+' {
			// <-containedInPlace+
			arc.SingleProp = expr[0:i]
			arc.Wildcard = "+"
			expr = expr[i+1:]
			break
		}
		if expr[i] == '{' {
			// <-containedInPlace{p:v}
			arc.SingleProp = expr[0:i]
			expr = expr[i:]
			break
		}
	}
	// {prop1:val1, prop2:val2}
	if len(expr) > 0 && expr[0] == '{' {
		if expr[len(expr)-1] != '}' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid filter string: %s", expr)
		}
		filter := map[string]string{}
		parts := strings.Split(replacer.Replace(expr[1:len(expr)-1]), ",")
		for _, p := range parts {
			if p == "" {
				continue
			}
			kv := strings.Split(p, ":")
			if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", p)
			}
			filter[kv[0]] = kv[1]
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
