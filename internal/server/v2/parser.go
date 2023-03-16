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

// splitArc splits query string by "->" and "<-" into arcs
func splitArc(s string) ([]string, error) {
	if len(s) < 2 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid query string: %s", s)
	}
	if s[0:2] != "->" && s[0:2] != "<-" {
		return nil, status.Errorf(
			codes.InvalidArgument, "query string should start with arrow, %s", s)
	}
	pos := []int{}
	for i := 0; i < len(s)-2; i++ {
		if s[i:i+2] == "->" || s[i:i+2] == "<-" {
			pos = append(pos, i)
		}
	}
	parts := []string{}
	for i := 0; i < len(pos)-1; i++ {
		parts = append(parts, s[pos[i]:pos[i+1]])
	}
	if len(pos) > 0 {
		parts = append(parts, s[pos[len(pos)-1]:])
	}
	return parts, nil
}

func parseArc(s string) (*Arc, error) {
	if len(s) < 2 {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid arc string: %s", s)
	}
	arc := &Arc{}
	if s[0:2] == "->" {
		arc.out = true
	} else if s[0:2] == "<-" {
		arc.out = false
	} else {
		return nil, status.Errorf(
			codes.InvalidArgument, "arc string should start with arrow, %s", s)
	}
	s = s[2:]
	// No property defined; This is to fetch all the properties.
	if len(s) == 0 {
		return arc, nil
	}
	// [prop1, prop2]
	if s[0] == '[' {
		if s[len(s)-1] != ']' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid filter string: %s", s)
		}
		s = s[1 : len(s)-1]
		arc.bracketProps = strings.Split(strings.ReplaceAll(s, " ", ""), ",")
		return arc, nil
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '+' {
			// <-containedInPlace+
			arc.singleProp = s[0:i]
			arc.wildcard = "+"
			s = s[i+1:]
			break
		}
		if s[i] == '{' {
			// <-containedInPlace{p:v}
			arc.singleProp = s[0:i]
			s = s[i:]
			break
		}
	}
	// {prop1:val1, prop2:val2}
	if len(s) > 0 && s[0] == '{' {
		if s[len(s)-1] != '}' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid filter string: %s", s)
		}
		filter := map[string]string{}
		parts := strings.Split(strings.ReplaceAll(s[1:len(s)-1], " ", ""), ",")
		for _, p := range parts {
			kv := strings.Split(p, ":")
			if len(kv) != 2 {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", p)
			}
			filter[kv[0]] = kv[1]
		}
		arc.filter = filter
		return arc, nil
	}
	// No '+' or '{' found, this is a single property.
	if len(s) > 0 {
		arc.singleProp = s
	}
	return arc, nil
}
