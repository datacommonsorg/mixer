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

package e2e

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

const numTestTimes = 4

// TestDriver drives various tests based on environment flags.
func TestDriver(
	apiName string,
	opt *TestOption,
	testSuite func(pb.MixerClient, bool, bool)) error {
	if LatencyTest {
		return latencyTest(apiName, opt, testSuite)
	}
	return goldenTest(opt, testSuite)
}

func goldenTest(opt *TestOption, testSuite func(pb.MixerClient, bool, bool)) error {
	for _, useImportGroup := range []bool{true, false} {
		opt.UseImportGroup = useImportGroup

		client, _, err := Setup(opt)
		if err != nil {
			return fmt.Errorf("failed to set up mixer and client")
		}

		testSuite(client, false /* latencyTest */, useImportGroup)
	}
	return nil
}

func latencyTest(
	apiName string,
	opt *TestOption,
	testSuite func(pb.MixerClient, bool, bool)) error {
	// Map: useImportGroup -> [duration in seconds].
	durationStore := map[bool][]float64{}

	for _, useImportGroup := range []bool{true, false} {
		opt.UseImportGroup = useImportGroup

		client, _, err := Setup(opt)
		if err != nil {
			return fmt.Errorf("failed to set up mixer and client")
		}

		// Run multiple times to reduce fluctuations.
		for i := 0; i < numTestTimes; i++ {
			startTime := time.Now()
			testSuite(client, true /* latencyTest */, useImportGroup)
			durationStore[useImportGroup] = append(durationStore[useImportGroup],
				time.Since(startTime).Seconds())
		}
	}

	oldValue := meanValue(durationStore[false])
	newValue := meanValue(durationStore[true])
	changeSign := ""
	if newValue > oldValue {
		changeSign = "+"
	}
	resultCsvRow := fmt.Sprintf("%s,%.2f,%.2f,%s%.2f%%\n",
		apiName, oldValue, newValue, changeSign, (newValue/oldValue-1)*100)

	fmt.Println(resultCsvRow)

	_, filename, _, _ := runtime.Caller(0)
	resultFilePath := path.Join(
		path.Dir(filename), "ig_latency", fmt.Sprintf("%s.csv", apiName))
	return os.WriteFile(resultFilePath, []byte(resultCsvRow), 0644)
}

func meanValue(list []float64) float64 {
	res := 0.0
	for _, item := range list {
		res += item
	}
	return res / float64(len(list))
}
