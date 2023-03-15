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
	if s[0] == '[' {
		if s[len(s)-1] != ']' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid filter string: %s", s)
		}
		s = s[1 : len(s)-1]
		// [prop1, prop2]
		props := []string{}
		pos := -1
		for i := 0; i < len(s)-1; i++ {
			if s[i] == ',' {
				props = append(props, strings.TrimSpace(s[pos+1:i]))
				pos = i
			}
		}
		props = append(props, strings.TrimSpace(s[pos+1:]))
		arc.props = props
		return arc, nil
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '+' {
			arc.prop = s[0:i]
			arc.wildcard = "+"
			s = s[i+1:]
			break
		} else if s[i] == '{' {
			arc.prop = s[0:i]
			s = s[i:]
			break
		}
	}
	if len(s) > 0 && s[0] == '{' {
		if s[len(s)-1] != '}' {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid filter string: %s", s)
		}
		// {prop1:val1, prop2:val2}
		filter := map[string]string{}
		parts := strings.Split(s[1:len(s)-1], ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			kv := []int{}
			for j := 0; j < len(p); j++ {
				if p[j] == ':' {
					kv = append(kv, j)
				}
			}
			if len(kv) != 1 {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", p)
			}
			k := strings.TrimSpace(p[0:kv[0]])
			v := strings.TrimSpace(p[kv[0]+1:])
			if len(k) == 0 || len(v) == 0 {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid filter string: %s", p)
			}
			filter[k] = v
		}
		arc.filter = filter
		return arc, nil
	}
	// No '+' or '{' found, this is a pure property.
	if len(s) > 0 {
		arc.prop = s
	}
	return arc, nil
}
