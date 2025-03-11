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

import "github.com/datacommonsorg/mixer/internal/util"

const (
	// Indicates that all properties should be returned.
	WILDCARD = "*"
)

// Arc represents an arc in the graph.
type Arc struct {
	// Whether it's out or in arc.
	Out bool
	// The property of the arc. This is when property is specified without []
	SingleProp string
	// The decorator used for the single property.
	Decorator string
	// The properties of the arc. This is when property is specified with []
	BracketProps []string
	// The filter of the arc: filter key -> filter values.
	Filter map[string][]string
}

func (arc *Arc) Direction() string {
	if arc.Out {
		return util.DirectionOut
	}
	return util.DirectionIn
}

func (arc *Arc) IsNodePropertiesArc() bool {
	return arc.SingleProp == "" && len(arc.BracketProps) == 0
}

func (arc *Arc) IsPropertyValuesArc() (bool, []string) {
	switch {
	case arc.SingleProp == WILDCARD:
		return true, []string{}
	case arc.SingleProp != "" && arc.Decorator == "":
		return true, []string{arc.SingleProp}
	case len(arc.BracketProps) > 0:
		return true, arc.BracketProps
	default:
		return false, []string{}
	}
}

// LinkedNodes represents a local graph starting from a node with connected arcs.
type LinkedNodes struct {
	Subject string
	Arcs    []*Arc
}
