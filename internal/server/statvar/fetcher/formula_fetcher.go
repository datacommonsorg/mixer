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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	v1pv "github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
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
		data, _, err := v1pv.Fetch(
			errCtx,
			store,
			[]string{"StatisticalCalculation"},
			[]string{"typeOf"},
			0,
			"",
			"in",
		)
		if err != nil {
			return err
		}
		statCal := []string{}
		for _, nodes := range data["StatisticalCalculation"]["typeOf"] {
			for _, node := range nodes {
				statCal = append(statCal, node.Dcid)
			}
		}
		if len(statCal) == 0 {
			return nil
		}
		resp, _, err := v1pv.Fetch(
			errCtx,
			store,
			statCal,
			[]string{"outputProperty", "inputPropertyExpression"},
			0,
			"",
			"out",
		)
		if err != nil {
			return err
		}
		// Wrap result in pbv2.NodeResponse
		localResp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		for dcid, data := range resp {
			localResp.Data[dcid] = &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{},
			}
			for prop, nodes := range data {
				if len(nodes) > 0 {
					localResp.Data[dcid].Arcs[prop] = &pbv2.Nodes{
						Nodes: v1pv.MergeTypedNodes(nodes),
					}
				}
			}
		}
		localRespChan <- localResp
		return nil
	})
	// Fetch for Remote Mixer.
	if metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			req := &pbv2.NodeRequest{
				Nodes:    []string{"StatisticalCalculation"},
				Property: "<-typeOf",
			}
			statCalResp := &pbv2.NodeResponse{}
			err := util.FetchRemote(metadata, &http.Client{}, "/v2/node", req, statCalResp)
			if err != nil {
				return err
			}
			statCal := []string{}
			for _, node := range statCalResp.Data["StatisticalCalculation"].Arcs["typeOf"].Nodes {
				statCal = append(statCal, node.Dcid)
			}
			if len(statCal) == 0 {
				return nil
			}
			req = &pbv2.NodeRequest{
				Nodes:    statCal,
				Property: "->[outputProperty, inputPropertyExpression]",
			}
			remoteResp := &pbv2.NodeResponse{}
			err = util.FetchRemote(metadata, &http.Client{}, "/v2/node", req, remoteResp)
			if err != nil {
				return err
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
