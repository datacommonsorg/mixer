// Copyright 2025 Google LLC
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

package remote

import (
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestUpdateNodeRequestNextToken(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		req           *pbv2.NodeRequest
		id            string
		wantNextToken string
	}{
		{
			&pbv2.NodeRequest{},
			"remote",
			"",
		},
		{
			&pbv2.NodeRequest{
				NextToken: "H4sIAAAAAAAA/+Li5+IoLkjMy0stMpRi5uhQRxIwkmLmeMEOAAAA//8BAAD//6VaKZYiAAAA",
			},
			"remote",
			"",
		},
		{
			&pbv2.NodeRequest{
				NextToken: "H4sIAAAAAAAA/+Li5+IoLkjMy0stMpRi5uhQ55Lh4kjKTC9JTMpJFRLg4uNiyU6tNBJi42ASYJLgQlJuJMXM8YKdS4qLrSg1N78kVUmgJLW4RKG4pCgzL10hMy8tHwAAAP//AQAA///cX0j1XAAAAA==",
			},
			"remote",
			"test string info",
		},
	} {
		err := updateNodeRequestNextToken(c.req, c.id)
		if err != nil {
			t.Errorf("Error running updateNodeRequestNextToken(%v, %v)", c.req, c.id)
		}
		if diff := cmp.Diff(c.req.GetNextToken(), c.wantNextToken, cmpOpts); diff != "" {
			t.Errorf("updateNodeRequestNextToken(%v, %v) got nextToken diff: %s", c.req, c.id, diff)
		}
	}
}

func TestUpdateNodeResponseNextToken(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		resp          *pbv2.NodeResponse
		id            string
		wantNextToken string
	}{
		{
			&pbv2.NodeResponse{},
			"remote",
			"",
		},
		{
			&pbv2.NodeResponse{
				NextToken: "test string info",
			},
			"remote",
			"H4sIAAAAAAAA/+KS4mIrSs3NL0lVEihJLS5RKC4pysxLV8jMS8sHAAAA//8BAAD//69wDuAcAAAA",
		},
	} {
		err := updateNodeResponseNextToken(c.resp, c.id)
		if err != nil {
			t.Errorf("Error running updateNodeResponseNextToken(%v, %v)", c.resp, c.id)
		}
		if diff := cmp.Diff(c.resp.GetNextToken(), c.wantNextToken, cmpOpts); diff != "" {
			t.Errorf("updateNodeResponseNextToken(%v, %v) got nextToken diff: %s", c.resp, c.id, diff)
		}
	}
}
