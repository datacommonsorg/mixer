// Copyright 2024 Google LLC
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

package cache

import (
	"encoding/json"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

func parseStatVarGroups(jsonData string) (*StatVarGroups, error) {
	var svgs StatVarGroups
	if err := json.Unmarshal([]byte(jsonData), &svgs); err != nil {
		return nil, err
	}
	return &svgs, nil
}

type StatVarGroups struct {
	StatVarGroups map[string]*StatVarGroupNode `json:"statVarGroups"`
}

func (svgs *StatVarGroups) toProto() *pb.StatVarGroups {
	nodes := map[string]*pb.StatVarGroupNode{}

	for id, node := range svgs.StatVarGroups {
		nodes[id] = node.toProto()
	}

	return &pb.StatVarGroups{StatVarGroups: nodes}
}

type StatVarGroupNode struct {
	AbsoluteName           string      `json:"absoluteName"`
	ChildStatVars          []*ChildSV  `json:"childStatVars"`
	ChildStatVarGroups     []*ChildSVG `json:"childStatVarGroups"`
	DescendentStatVarCount int32       `json:"descendentStatVarCount"`
}

func (node *StatVarGroupNode) toProto() *pb.StatVarGroupNode {
	var childSVs []*pb.StatVarGroupNode_ChildSV
	for _, childSV := range node.ChildStatVars {
		childSVs = append(childSVs, childSV.toProto())
	}

	var childSVGs []*pb.StatVarGroupNode_ChildSVG
	for _, childSVG := range node.ChildStatVarGroups {
		childSVGs = append(childSVGs, childSVG.toProto())
	}

	return &pb.StatVarGroupNode{
		AbsoluteName:           node.AbsoluteName,
		ChildStatVars:          childSVs,
		ChildStatVarGroups:     childSVGs,
		DescendentStatVarCount: node.DescendentStatVarCount,
	}
}

type ChildSVG struct {
	Id                string `json:"id"`
	SpecializedEntity string `json:"specializedEntity"`
}

func (childSVG *ChildSVG) toProto() *pb.StatVarGroupNode_ChildSVG {
	return &pb.StatVarGroupNode_ChildSVG{
		Id:                childSVG.Id,
		SpecializedEntity: childSVG.SpecializedEntity,
	}
}

type ChildSV struct {
	Id          string   `json:"id"`
	DisplayName string   `json:"displayName"`
	SearchNames []string `json:"searchNames"`
}

func (childSV *ChildSV) toProto() *pb.StatVarGroupNode_ChildSV {
	return &pb.StatVarGroupNode_ChildSV{
		Id:          childSV.Id,
		DisplayName: childSV.DisplayName,
		SearchNames: childSV.SearchNames,
	}
}
