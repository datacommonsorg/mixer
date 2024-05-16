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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	v1pv "github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

// V2Node for Remote Mixer.
func FetchRemoteNode(
	ctx context.Context,
	metadata *resource.Metadata,
	req *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	errGroup, _ := errgroup.WithContext(ctx)
	remoteResponseChan := make(chan *pbv2.NodeResponse, 1)
	errGroup.Go(func() error {
		remoteResp := &pbv2.NodeResponse{}
		err := util.FetchRemote(metadata, &http.Client{}, "/v2/node", req, remoteResp)
		if err != nil {
			return err
		}
		return nil
	})
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(remoteResponseChan)
	return <-remoteResponseChan, nil
}

// FetchFormulas fetches StatisticalCalculations from storage and returns a map of SV -> inputPropertyExpressions.
func FetchFormulas(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
) (map[string][]string, error) {
	result := map[string][]string{}
	svToCalculations := map[string][]string{}
	calculationToFormulas := map[string][]string{}
	// Fetch for BT and SQL.
	data, _, err := v1pv.Fetch(
		ctx,
		store,
		[]string{"StatisticalCalculation"},
		[]string{"typeOf"},
		0,
		"",
		"in",
	)
	if err != nil {
		return nil, err
	}
	statisticalCalculations := []string{}
	for _, nodes := range data["StatisticalCalculation"]["typeOf"] {
		for _, node := range nodes {
			statisticalCalculations = append(statisticalCalculations, node.Dcid)
		}
	}
	data, _, err = v1pv.Fetch(
		ctx,
		store,
		statisticalCalculations,
		[]string{"outputProperty", "inputPropertyExpression"},
		0,
		"",
		"out",
	)
	if err != nil {
		return nil, err
	}
	for dcid, properties := range data {
		for _, nodes := range properties["outputProperty"] {
			for _, node := range nodes {
				svToCalculations[node.Dcid] = append(svToCalculations[node.Dcid], dcid)
			}
		}
		for _, nodes := range properties["inputPropertyExpression"] {
			for _, node := range nodes {
				calculationToFormulas[dcid] = append(calculationToFormulas[dcid], node.Value)
			}
		}
	}
	// Fetch for Remote Mixer.
	if metadata.RemoteMixerDomain != "" {
		req := &pbv2.NodeRequest{
			Nodes:    []string{"StatisticalCalculation"},
			Property: "<-typeOf",
		}
		statisticalCalculationResp, err := FetchRemoteNode(ctx, metadata, req)
		if err != nil {
			return nil, err
		}
		statisticalCalculations := []string{}
		for _, node := range statisticalCalculationResp.Data["StatisticalCalculation"].Arcs["typeOf"].Nodes {
			statisticalCalculations = append(statisticalCalculations, node.Dcid)
		}
		req = &pbv2.NodeRequest{
			Nodes:    statisticalCalculations,
			Property: "->[outputProperty, inputPropertyExpression]",
		}
		propertyResp, err := FetchRemoteNode(ctx, metadata, req)
		if err != nil {
			return nil, err
		}
		for dcid, properties := range propertyResp.Data {
			for _, node := range properties.Arcs["outputProperty"].Nodes {
				svToCalculations[node.Dcid] = append(svToCalculations[node.Dcid], dcid)
			}
			for _, node := range properties.Arcs["inputPropertyExpression"].Nodes {
				calculationToFormulas[dcid] = append(calculationToFormulas[dcid], node.Value)
			}
		}
	}
	for sv, calculations := range svToCalculations {
		for _, calculation := range calculations {
			formulas, ok := calculationToFormulas[calculation]
			if ok {
				result[sv] = append(result[sv], formulas...)
			}
		}
	}
	// Sort for determinism.
	for _, formulas := range result {
		sort.Strings(formulas)
	}
	return result, nil
}
