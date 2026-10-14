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
	"github.com/datacommonsorg/mixer/internal/nodefetcher"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/resource"

	triples "github.com/datacommonsorg/mixer/internal/server/v1/triples"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

const (
	// Use large page size for cache, since this is internal and only used at startup.
	cachePageSize           = 5000
	inputPropertyExpression = "inputPropertyExpression"
	outputProperty          = "outputProperty"
	StatisticalCalculation  = "StatisticalCalculation"
	typeOf                  = "typeOf"
	v2node                  = "/v2/node"
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
		statCalDcids := []string{}
		nextToken := ""
		for {
			statCalReq := &pbv1.TriplesRequest{
				Node:      StatisticalCalculation,
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
			typeOf, ok := statCalResp.Triples[typeOf]
			if ok {
				for _, node := range typeOf.Nodes {
					statCalDcids = append(statCalDcids, node.Dcid)
				}
			}
			nextToken = statCalResp.GetNextToken()
			if nextToken == "" {
				break
			}
		}
		if len(statCalDcids) == 0 {
			return nil
		}
		localResp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		for {
			bulkReq := &pbv1.BulkTriplesRequest{
				Nodes:     statCalDcids,
				Direction: "out",
				NextToken: nextToken,
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
				out, outOk := nodeTriples.Triples[outputProperty]
				in, inOk := nodeTriples.Triples[inputPropertyExpression]
				if !outOk || !inOk {
					continue
				}
				localResp.Data[nodeTriples.GetNode()] = &pbv2.LinkedGraph{
					Arcs: map[string]*pbv2.Nodes{
						outputProperty:          {Nodes: out.Nodes},
						inputPropertyExpression: {Nodes: in.Nodes},
					},
				}
			}
			nextToken = bulkResp.GetNextToken()
			if nextToken == "" {
				break
			}
		}
		localRespChan <- localResp
		return nil
	})
	// Fetch for Remote Mixer.
	if metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.NodeResponse{}
			statCalDcids := []string{}
			nextToken := ""
			for {
				statCalReq := &pbv2.NodeRequest{
					Nodes:     []string{StatisticalCalculation},
					Property:  "<-" + typeOf,
					NextToken: nextToken,
				}
				statCalResp := &pbv2.NodeResponse{}
				err := util.FetchRemote(errCtx, metadata, &http.Client{}, v2node, statCalReq, statCalResp)
				if err != nil {
					return err
				}
				for _, node := range statCalResp.GetData()[StatisticalCalculation].GetArcs()[typeOf].GetNodes() {
					statCalDcids = append(statCalDcids, node.GetDcid())
				}
				nextToken = statCalResp.GetNextToken()
				if nextToken == "" {
					break
				}
			}
			if len(statCalDcids) == 0 {
				return nil
			}
			for {
				currResp := &pbv2.NodeResponse{}
				propReq := &pbv2.NodeRequest{
					Nodes:     statCalDcids,
					Property:  "->[" + outputProperty + ", " + inputPropertyExpression + "]",
					NextToken: nextToken,
				}
				err := util.FetchRemote(errCtx, metadata, &http.Client{}, v2node, propReq, currResp)
				if err != nil {
					return err
				}
				remoteResp, err = merger.MergeNode(remoteResp, currResp)
				if err != nil {
					return err
				}
				nextToken = currResp.GetNextToken()
				if nextToken == "" {
					break
				}
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
	return formulasFromNodeResponse(mergedResp), nil
}

// FetchSVFormulas returns a map of StatisticalVariable dcids to a slice of corresponding inputPropertyExpressions.
func FetchSVFormulas(
	ctx context.Context,
	ds *datasources.DataSources,
) (map[string][]string, error) {
	// Fetch all StatisticalCalculation dcids.
	dcids, err := fetchStatisticalCalculationDcids(ctx, ds)
	if err != nil {
		return nil, err
	}
	if len(dcids) == 0 {
		return nil, nil
	}

	// Fetch outputProperty and inputPropertyExpression for the StatisticalCalculations.
	return fetchStatisticalCalculationProperties(ctx, ds, dcids)
}

// fetchStatisticalCalculationDcids returns a slice of StatisticalCalculation dcids.
func fetchStatisticalCalculationDcids(
	ctx context.Context,
	ds *datasources.DataSources,
) ([]string, error) {
	resp, err := fetchAllNodePages(ctx, ds, &pbv2.NodeRequest{
		Nodes:    []string{StatisticalCalculation},
		Property: "<-" + typeOf,
	})
	if err != nil {
		return nil, err
	}
	nodes, ok := resp.GetData()[StatisticalCalculation].GetArcs()[typeOf]
	if !ok {
		return nil, nil
	}
	dcids := []string{}
	for _, node := range nodes.GetNodes() {
		dcids = append(dcids, node.GetDcid())
	}
	return dcids, nil
}

// fetchStatisticalCalculationProperties fetches properties for StatisticalCalculation dcids.
// Returns a map of outputProperty to inputPropertyExpression(s).
func fetchStatisticalCalculationProperties(
	ctx context.Context,
	ds *datasources.DataSources,
	dcids []string,
) (map[string][]string, error) {
	// Read every page before matching properties: a node's two properties can
	// land on different pages.
	resp, err := fetchAllNodePages(ctx, ds, &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[" + outputProperty + ", " + inputPropertyExpression + "]",
	})
	if err != nil {
		return nil, err
	}
	return formulasFromNodeResponse(resp), nil
}

// formulasFromNodeResponse maps each outputProperty dcid in a
// StatisticalCalculation Node response to its inputPropertyExpression values.
// A node missing either property adds nothing: GetNodes returns nil for a
// missing or nil arc.
func formulasFromNodeResponse(resp *pbv2.NodeResponse) map[string][]string {
	result := map[string][]string{}
	for _, props := range resp.GetData() {
		arcs := props.GetArcs()
		for _, outputNode := range arcs[outputProperty].GetNodes() {
			dcid := outputNode.GetDcid()
			for _, inputNode := range arcs[inputPropertyExpression].GetNodes() {
				result[dcid] = append(result[dcid], inputNode.GetValue())
			}
		}
	}
	// Sort for determinism.
	for _, formulas := range result {
		sort.Strings(formulas)
	}
	return result
}

// fetchAllNodePages reads every page of a Node request from ds and merges them.
func fetchAllNodePages(
	ctx context.Context,
	ds *datasources.DataSources,
	req *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	return nodefetcher.NodeFetchAllFunc(ctx, func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return ds.Node(ctx, req, cachePageSize)
	}, req)
}
