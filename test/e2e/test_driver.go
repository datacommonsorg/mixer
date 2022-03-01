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
)

const numTestTimes = 4

// TestDriver drives various tests based on environment flags.
func TestDriver(
	apiName string,
	opt *TestOption,
	testSuite func(*TestOption, bool)) error {
	if LatencyTest {
		return latencyTest(apiName, opt, testSuite)
	}
	goldenTest(opt, testSuite)
	return nil
}

func goldenTest(opt *TestOption, testSuite func(*TestOption, bool)) {
	for _, useImportGroup := range []bool{true, false} {
		opt.UseImportGroup = useImportGroup
		testSuite(opt, false /* latencyTest */)
	}
}

func latencyTest(
	apiName string,
	opt *TestOption,
	testSuite func(*TestOption, bool)) error {
	// Map: useImportGroup -> [duration in seconds].
	durationStore := map[bool][]float64{}

	// Run multiple times to reduce fluctuations.
	for i := 0; i < numTestTimes; i++ {
		for _, useImportGroup := range []bool{true, false} {
			opt.UseImportGroup = useImportGroup

			startTime := time.Now()
			testSuite(opt, true /* latencyTest */)
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
