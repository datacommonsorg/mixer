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
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// BuildCursor is a wrapper function to build Cursor.
func BuildCursor(ig, page, item int32) *pb.Cursor {
	return &pb.Cursor{Ig: ig, Page: page, Item: item}
}

// Decode decodes a compressed token string into PaginationInfo.
func Decode(s string) (*pb.PaginationInfo, error) {
	data, err := util.UnzipAndDecode(s)
	if err != nil {
		return nil, err
	}
	result := &pb.PaginationInfo{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
