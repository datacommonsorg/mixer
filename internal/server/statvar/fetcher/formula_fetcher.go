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

package fetcher

import (
	"context"
	"net/http"
	"sort"

	"github.com/datacommonsorg/mixer/internal/merger"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"

	triples "github.com/datacommonsorg/mixer/internal/server/v1/triples"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

// FetchFormulas fetches StatisticalCalculations and returns a map of SV dcids to a list of inputPropertyExpressions.
func FetchFormulas(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
) (map[string][]string, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.NodeResponse, 1)
	remoteRespChan := make(chan *pbv2.NodeResponse, 1)
	// Fetch for BT and SQL.
	errGroup.Go(func() error {
		statCal := []string{}
		nextToken := ""
		for ok := true; ok; ok = (nextToken != "") {
			statCalReq := &pbv1.TriplesRequest{
				Node:      "StatisticalCalculation",
				Direction: "in",
				NextToken: nextToken,
			}
			statCalResp, err := triples.Triples(
				errCtx,
				statCalReq,
				store,
				metadata,
			)
			if err != nil {
				return err
			}
			typeOf, ok := statCalResp.Triples["typeOf"]
			if ok {
				for _, node := range typeOf.Nodes {
					statCal = append(statCal, node.Dcid)
				}
			}
			nextToken = statCalResp.GetNextToken()
		}
		if len(statCal) == 0 {
			return nil
		}
		localResp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		for ok := true; ok; ok = (nextToken != "") {
			bulkReq := &pbv1.BulkTriplesRequest{
				Nodes:     statCal,
				Direction: "out",
			}
			bulkResp, err := triples.BulkTriples(
				errCtx,
				bulkReq,
				store,
				metadata,
			)
			if err != nil {
				return err
			}
			// Wrap result in pbv2.NodeResponse.
			for _, nodeTriples := range bulkResp.Data {
				out, outOk := nodeTriples.Triples["outputProperty"]
				in, inOk := nodeTriples.Triples["inputPropertyExpression"]
				if !outOk || !inOk {
					continue
				}
				localResp.Data[nodeTriples.GetNode()] = &pbv2.LinkedGraph{
					Arcs: map[string]*pbv2.Nodes{
						"outputProperty":          {Nodes: out.Nodes},
						"inputPropertyExpression": {Nodes: in.Nodes},
					},
				}
			}
			nextToken = bulkResp.GetNextToken()
		}
		localRespChan <- localResp
		return nil
	})
	// Fetch for Remote Mixer.
	if metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.NodeResponse{}
			statCal := []string{}
			nextToken := ""
			for ok := true; ok; ok = (nextToken != "") {
				statCalReq := &pbv2.NodeRequest{
					Nodes:     []string{"StatisticalCalculation"},
					Property:  "<-typeOf",
					NextToken: nextToken,
				}
				statCalResp := &pbv2.NodeResponse{}
				err := util.FetchRemote(metadata, &http.Client{}, "/v2/node", statCalReq, statCalResp)
				if err != nil {
					return err
				}
				for _, node := range statCalResp.Data["StatisticalCalculation"].Arcs["typeOf"].Nodes {
					statCal = append(statCal, node.Dcid)
				}
				nextToken = statCalResp.GetNextToken()
			}
			if len(statCal) == 0 {
				return nil
			}
			for ok := true; ok; ok = (nextToken != "") {
				currResp := &pbv2.NodeResponse{}
				propReq := &pbv2.NodeRequest{
					Nodes:    statCal,
					Property: "->[outputProperty, inputPropertyExpression]",
				}
				err := util.FetchRemote(metadata, &http.Client{}, "/v2/node", propReq, currResp)
				if err != nil {
					return err
				}
				remoteResp, err = merger.MergeNode(remoteResp, currResp)
				if err != nil {
					return err
				}
				nextToken = currResp.GetNextToken()
			}
			remoteRespChan <- remoteResp
			return nil
		})
	} else {
		remoteRespChan <- nil
	}
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localRespChan)
	close(remoteRespChan)
	localResp, remoteResp := <-localRespChan, <-remoteRespChan
	mergedResp, err := merger.MergeNode(localResp, remoteResp)
	if err != nil {
		return nil, err
	}
	result := map[string][]string{}
	for _, props := range mergedResp.Data {
		// Skip nodes missing required properties.
		_, out := props.Arcs["outputProperty"]
		_, in := props.Arcs["inputPropertyExpression"]
		if !(out && in) {
			continue
		}
		for _, outputNode := range props.Arcs["outputProperty"].Nodes {
			for _, inputNode := range props.Arcs["inputPropertyExpression"].Nodes {
				result[outputNode.Dcid] = append(result[outputNode.Dcid], inputNode.Value)
			}
		}
	}
	// Sort for determinism.
	for _, formulas := range result {
		sort.Strings(formulas)
	}
	return result, nil
}
