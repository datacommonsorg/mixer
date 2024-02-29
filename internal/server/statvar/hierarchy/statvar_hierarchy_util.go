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

package hierarchy

import (
	"encoding/json"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/datacommonsorg/mixer/internal/server/resource"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
)

const (
	// SvgRoot is the root stat var group of the hierarchy. It's a virtual entity
	// that links to the top level category stat var groups.
	SvgRoot         = "dc/g/Root"
	CustomSvgRoot   = "dc/g/Custom_Root"
	CustomSVGPrefix = "dc/g/Custom_"
)

// Note this function modifies validSVG inside.
func markValidSVG(
	input map[string]*pb.StatVarGroupNode,
	svgID string,
	validSVG map[string]struct{},
) bool {
	// Already checked
	if _, ok := validSVG[svgID]; ok {
		return true
	}
	svChildren := input[svgID].ChildStatVars
	svgChildren := input[svgID].ChildStatVarGroups
	// If there are non-empty sv chldren, then this svg is valid
	if len(svChildren) > 0 {
		validSVG[svgID] = struct{}{}
		return true
	}
	// Recursively check child svg, if there is any valid svg child, then this
	// is valid too
	for _, svgChild := range svgChildren {
		if markValidSVG(input, svgChild.Id, validSVG) {
			validSVG[svgID] = struct{}{}
			return true
		}
	}
	return false
}

// FilterSVG filters StatVarGroups based on given stat vars. This does not modify the input
// data but create a filtered copy of it.
func FilterSVG(
	input map[string]*pb.StatVarGroupNode,
	statVars []string,
) map[string]*pb.StatVarGroupNode {
	result := map[string]*pb.StatVarGroupNode{}
	// Build set for all the given stat vars as valid stat vars.
	validSV := map[string]struct{}{}
	for _, sv := range statVars {
		validSV[sv] = struct{}{}
	}

	// Step 1: iterate over stat var group, and only keep stat var children with
	// valid stat vars.
	for svgID, svgData := range input {
		filteredChildSV := []*pb.StatVarGroupNode_ChildSV{}
		for _, childSV := range svgData.ChildStatVars {
			if _, ok := validSV[childSV.Id]; ok {
				filteredChildSV = append(filteredChildSV, childSV)
			}
		}
		result[svgID] = &pb.StatVarGroupNode{
			ChildStatVars:      filteredChildSV,
			ChildStatVarGroups: svgData.ChildStatVarGroups,
		}
	}
	// Step 2: recursively check if a stat var group is valid. A stat var group
	// is valid if it has any descendent stat var group with non-empty stat vars

	// All the svg with valid stat vars.
	validSVG := map[string]struct{}{}
	for svgID := range result {
		markValidSVG(result, svgID, validSVG)
	}

	// Step3: another iteration to only keep valid svg
	for svgID, svgData := range result {
		filteredChildren := []*pb.StatVarGroupNode_ChildSVG{}
		for _, c := range svgData.ChildStatVarGroups {
			if _, ok := validSVG[c.Id]; ok {
				filteredChildren = append(filteredChildren, c)
			}
		}
		result[svgID].ChildStatVarGroups = filteredChildren
		d := result[svgID]
		if len(d.ChildStatVars) == 0 && len(d.ChildStatVarGroups) == 0 {
			delete(result, svgID)
		}
	}
	return result
}

// RemoveSvg removes the blocked svg from the in-memory entries.
func RemoveSvg(
	svgNodeMap map[string]*pb.StatVarGroupNode,
	parentSvgMap map[string][]string,
	svg string,
) {
	if node, ok := svgNodeMap[svg]; ok {
		for _, child := range node.ChildStatVarGroups {
			RemoveSvg(svgNodeMap, parentSvgMap, child.Id)
		}
	}
	for _, parent := range parentSvgMap[svg] {
		if node, ok := svgNodeMap[parent]; ok {
			children := []*pb.StatVarGroupNode_ChildSVG{}
			for _, child := range node.ChildStatVarGroups {
				if child.Id != svg {
					children = append(children, child)
				}
			}
			node.ChildStatVarGroups = children
		}
	}
	delete(svgNodeMap, svg)
}

// BuildParentSvgMap gets the mapping of svg/sv id to the parent svg for that
// svg/sv. Only gets the parent svg that have a path to the root node.
func BuildParentSvgMap(rawSvg map[string]*pb.StatVarGroupNode) map[string][]string {
	parentSvgMap := map[string][]string{}
	// Do breadth first search starting at the root to find all the svg that have
	// a path to the root. Only add those as parents.
	seenSvg := map[string]struct{}{SvgRoot: {}}
	svgToVisit := []string{SvgRoot}
	for len(svgToVisit) > 0 {
		svgID := svgToVisit[0]
		svgToVisit = svgToVisit[1:]
		if svgData, ok := rawSvg[svgID]; ok {
			for _, childSvg := range svgData.ChildStatVarGroups {
				// Add the current svg to the list of parents for this child svg.
				if _, ok := parentSvgMap[childSvg.Id]; !ok {
					parentSvgMap[childSvg.Id] = []string{}
				}
				parentSvgMap[childSvg.Id] = append(parentSvgMap[childSvg.Id], svgID)
				// If this child svg hasn't been seen yet, add it to the list of svg
				// left to visit.
				if _, ok := seenSvg[childSvg.Id]; !ok {
					seenSvg[childSvg.Id] = struct{}{}
					svgToVisit = append(svgToVisit, childSvg.Id)
				}
			}
			// Add the current svg to the list of parents for each child sv.
			for _, childSv := range svgData.ChildStatVars {
				if _, ok := parentSvgMap[childSv.Id]; !ok {
					parentSvgMap[childSv.Id] = []string{}
				}
				parentSvgMap[childSv.Id] = append(parentSvgMap[childSv.Id], svgID)
			}
		}
	}
	for _, parentSvgList := range parentSvgMap {
		sort.Strings(parentSvgList)
	}
	return parentSvgMap
}

// Helper to build a set of ignored SVGs.
func getIgnoredSVGHelper(
	ignoredSvg map[string]string,
	rawSvg map[string]*pb.StatVarGroupNode,
	svgID string) {
	ignoredSvg[svgID] = ""
	if svgData, ok := rawSvg[svgID]; ok {
		for _, svData := range svgData.ChildStatVarGroups {
			getIgnoredSVGHelper(ignoredSvg, rawSvg, svData.Id)
		}
	}
}

func getSynonymMap() map[string][]string {
	synonymMap := map[string][]string{}
	_, filename, _, _ := runtime.Caller(0)
	bytes, err := os.ReadFile(path.Join(path.Dir(filename), "../resource/synonyms.json"))
	if err == nil {
		var synonyms [][]string
		err = json.Unmarshal(bytes, &synonyms)
		if err == nil {
			// for each synonymList (list of words that are all synonyms of each other),
			// append that list to the synonymMap value for each word in the list.
			for _, synonymList := range synonyms {
				for _, word := range synonymList {
					if _, ok := synonymMap[word]; !ok {
						synonymMap[word] = synonymList
					} else {
						synonymMap[word] = append(synonymMap[word], synonymList...)
					}
				}
			}
		}
	}
	return synonymMap
}

// BuildStatVarSearchIndex builds the search index for the stat var hierarchy.
func BuildStatVarSearchIndex(
	rawSvg map[string]*pb.StatVarGroupNode,
	parentSvg map[string][]string,
	ignoredSvgIds map[string]struct{},
) *resource.SearchIndex {
	defer util.TimeTrack(time.Now(), "BuildStatVarSearchIndex")
	// map of token to map of sv/svg id to ranking information.
	searchIndex := &resource.SearchIndex{
		RootTrieNode: &resource.TrieNode{},
		Ranking:      map[string]*resource.RankingInfo{},
	}
	ignoredSVG := map[string]string{}
	// Exclude svg and sv under miscellaneous from the search index
	for svgID := range ignoredSvgIds {
		if svgID == "" {
			continue
		}
		getIgnoredSVGHelper(ignoredSVG, rawSvg, svgID)
	}
	synonymMap := getSynonymMap()
	seenSV := map[string]struct{}{}
	for svgID, svgData := range rawSvg {
		if _, ok := ignoredSVG[svgID]; ok {
			continue
		}
		// Ignore svg that don't have any parents with a path to the root node.
		if _, ok := parentSvg[svgID]; !ok {
			continue
		}
		for _, svData := range svgData.ChildStatVars {
			if _, ok := seenSV[svData.Id]; ok {
				continue
			}
			seenSV[svData.Id] = struct{}{}
			svTokenString := strings.Join(svData.SearchNames, " ")
			searchIndex.Update(svData.Id, svTokenString, svData.DisplayName, synonymMap, svData.Definition)
		}
	}
	return searchIndex
}

// AdjustDescendentSVCount returns all the unique stat var under curID.
// Recursively mutates DescendentStatVarCount for all nodes in id2Node.
func AdjustDescendentSVCount(
	id2Node map[string]*pb.StatVarGroupNode, curID string,
) map[string]struct{} {
	curNode, ok := id2Node[curID]
	if !ok {
		return nil
	}
	descendentSV := map[string]struct{}{}
	for _, sv := range curNode.ChildStatVars {
		descendentSV[sv.Id] = struct{}{}
	}
	for _, childSVG := range curNode.GetChildStatVarGroups() {
		childDescendentSV := AdjustDescendentSVCount(id2Node, childSVG.GetId())
		// Child SVG protos have its own DescendentStatVarCount that must be set.
		childSVG.DescendentStatVarCount = int32(len(childDescendentSV))
		for sv := range childDescendentSV {
			descendentSV[sv] = struct{}{}
		}
	}
	curNode.DescendentStatVarCount = int32(len(descendentSV))
	return descendentSV
}

// MergeSVGNodes merges node2's svg and svs into node1.
// Merge here refers to set operation.
// Warning: node1's DescendentStatVarCount is unchanged.
// Caller is responsible for calling fixCustomRootDescendentStatVarCount
// at the end of all merges.
func MergeSVGNodes(node1, node2 *pb.StatVarGroupNode) {
	node1SVGs := map[string]bool{}
	for _, childSVG := range node1.GetChildStatVarGroups() {
		node1SVGs[childSVG.GetId()] = true
	}
	for _, childSVG := range node2.GetChildStatVarGroups() {
		if _, ok := node1SVGs[childSVG.GetId()]; !ok {
			node1.ChildStatVarGroups = append(node1.ChildStatVarGroups, childSVG)
			node1SVGs[childSVG.GetId()] = true
		}
	}

	node1SVs := map[string]bool{}
	for _, childSV := range node1.GetChildStatVars() {
		node1SVs[childSV.GetId()] = true
	}
	for _, childSV := range node2.GetChildStatVars() {
		if _, ok := node1SVs[childSV.GetId()]; !ok {
			node1.ChildStatVars = append(node1.ChildStatVars, childSV)
			node1SVs[childSV.GetId()] = true
		}
	}
}
