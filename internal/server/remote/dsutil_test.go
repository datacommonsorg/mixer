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

func TestFormatNodeRequest(t *testing.T) {
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
				NextToken: "H4sIAAAAAAAA/+Li5+IoLkjMy0stMpRi5uhQ55LiYitKzc0vSRUS4OLjYslOrTQSYuNgEmCS4EJSbCTFzPGCHQAAAP//AQAA//+wr2EhPgAAAA==",
			},
			"remote",
			"H4sIAAAAAAAA/xIS4OLjYslOrTQSYuNgEmCS4AIAAAD//wEAAP//Uucl2BIAAAA=",
		},
	} {
		err := formatNodeRequest(c.req, c.id)
		if err != nil {
			t.Errorf("Error running formatNodeRequest(%v, %v)", c.req, c.id)
		}
		if diff := cmp.Diff(c.req.GetNextToken(), c.wantNextToken, cmpOpts); diff != "" {
			t.Errorf("formatNodeRequest(%v, %v) got nextToken diff: %s", c.req, c.id, diff)
		}
	}
}

func TestFormatNodeResponse(t *testing.T) {
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
				NextToken: "H4sIAAAAAAAA/xIS4OLjYslOrTQSYuNgEmCS4AIAAAD//wEAAP//Uucl2BIAAAA=",
			},
			"remote",
			"H4sIAAAAAAAA/+KS4WIrSs3NL0kVEhIS4OLjYslOrTQSYuNgEmCS4AIAAAD//wEAAP//vwkwFB4AAAA=",
		},
	} {
		err := formatNodeResponse(c.resp, c.id)
		if err != nil {
			t.Errorf("Error running formatNodeResponse(%v, %v)", c.resp, c.id)
		}
		if diff := cmp.Diff(c.resp.GetNextToken(), c.wantNextToken, cmpOpts); diff != "" {
			t.Errorf("formatNodeResponse(%v, %v) got nextToken diff: %s", c.resp, c.id, diff)
		}
	}
}
