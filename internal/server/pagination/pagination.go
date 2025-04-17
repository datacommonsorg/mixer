// Copyright 2022 Google LLC
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

package pagination

import (
	"fmt"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// Decode decodes a compressed token string into PaginationInfo.
func Decode(s string) (*pbv1.PaginationInfo, error) {
	if s == "" {
		return nil, fmt.Errorf("empty pagination token string")
	}

	data, err := util.UnzipAndDecode(s)
	if err != nil {
		return nil, err
	}
	result := &pbv1.PaginationInfo{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Decode decodes a compressed token string into Pagination.
func DecodeNextToken(s string) (*pbv2.Pagination, error) {
	if s == "" {
		return nil, fmt.Errorf("empty pagination token string")
	}

	data, err := util.UnzipAndDecode(s)
	if err != nil {
		return nil, err
	}
	result := &pbv2.Pagination{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
