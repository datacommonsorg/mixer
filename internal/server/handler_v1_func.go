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

// This file includes functions used by handler_v1 that can be mocked in tests.

package server

import (
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/v1/info"
	"github.com/datacommonsorg/mixer/internal/util"
)

var localBulkVariableInfoFunc = info.BulkVariableInfo

var remoteBulkVariableInfoFunc = func(
	s *Server,
	remoteReq *pbv1.BulkVariableInfoRequest,
) (*pbv1.BulkVariableInfoResponse, error) {
	remoteResp := &pbv1.BulkVariableInfoResponse{}
	return remoteResp, util.FetchRemote(
		s.metadata,
		s.httpClient,
		"/v1/bulk/info/variable",
		remoteReq,
		remoteResp,
	)
}
