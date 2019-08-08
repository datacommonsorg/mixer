// Copyright 2019 Google LLC
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

package store

import (
	"context"
	"fmt"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
)

func (s *store) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest,
	out *pb.GetPlaceObsResponse) error {
	key := in.GetPlaceType() + "-" + in.GetPopulationType()
	if len(in.GetPvs()) > 0 {
		util.IterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "-" + p + "-" + v
		})
	}
	btPrefix := fmt.Sprintf("%s%s", util.BtPlaceObsPrefix, key)
	btTable := s.btClient.Open(util.BtTable)

	// Query for the prefix.
	btRow, err := btTable.ReadRow(ctx, btPrefix)
	if err != nil {
		return err
	}
	if len(btRow[util.BtFamily]) > 0 && btRow[util.BtFamily][0].Row == btPrefix {
		out.Payload = string(btRow[util.BtFamily][0].Value)
	}
	return nil
}
