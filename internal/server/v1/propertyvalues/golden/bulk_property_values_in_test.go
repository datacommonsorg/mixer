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

package golden

import (
	"context"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_property_values_in")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			entities   []string
			limit      int32
			token      string
		}{
			{
				"containedIn1.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"",
			},
			{
				"containedIn2.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"H4sIAAAAAAAA/6SW/2/SQBjGpTLWlbnUqgmJiX+CY1P8Qd0XVqDrcAsOpsl+MUf7Wi457sj1uoX/3rQVHY67F+JPhJDnk+eee957cY6deiQyruR8/2bYdtxIcEUohzjkA0YicLaGiijwnnhVu+JWvKptuRXPsp96VbvqVpwQ07sjGkNAsgRyEBVci/qOod74wNMs9YWEM5JCXABTRSPC2hKIFtzFwLsl+BoSk78LDPM8ABFIGhfffhx0INGy0Ni3A0YiCvIPwbKtjY4UDny/NNJqam18wTAv2vGUcpoqSRS9gzzmAy2th9Ge+WI6zRTlya3g+k6douH45c9aQsepJyDCeL/5odl6t8rJMJuB/JnxeEgftHs55COMUvWp0pvoY/KXS31pvj1sPazMspcAg7lLsPf9Sw0IjaZeDsNIkkhpzzbCKK9Lyi2d+SKGERlnrJh+46CGaGLdGbmGmZB5h3okoqy8AMv+95jHGGqr+NBaOcH0zkDcgxwwwtVKA+jte49HS+vGx2B7bSq/ZiSP43ehHzu6RNNdvK8ZV/MOvaOp6TnsOfYC91/P/pGRs/2NMkYS/UVdGOWbHalvZL3yBU8kpDmCsE5+c9QwImdG2O4V0GQyFnIiRKxlfDIykAp/Noqx/p4b1ZuU99RIWryhtVzb2PFqubqxU+hrub6x41wZCY0ugylwReR8GE2EwK8mMPI2WOMdI2hvpR2roPyN2nJCI2XFCj7UGvpoznok7vXtbxu1a21MM2KtzWJuba2cZK0aae05TSZrduTEnMYgGzMa3Sj9BsK6kf+t9sV0JjhwrY1fAAAA//8BAAD//5gG1JigCwAA",
			},
			{
				"typeOf.json",
				"typeOf",
				[]string{"Country", "State", "City"},
				100,
				"",
			},
		} {
			req := &pb.BulkPropertyValuesRequest{
				Property:  c.property,
				Entities:  c.entities,
				Direction: util.DirectionIn,
				Limit:     c.limit,
				NextToken: c.token,
			}
			resp, err := mixer.BulkPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.BulkPropertyValues/in: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.BulkPropertyValuesResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("Golden got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"BulkPropertyValues/in", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
