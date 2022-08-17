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
// noop

package test

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
	testSuite func(pb.MixerClient, pb.ReconClient, bool),
) error {
	if LatencyTest {
		return latencyTest(apiName, opt, testSuite)
	}
	return goldenTest(opt, testSuite)
}

func goldenTest(
	opt *TestOption,
	testSuite func(pb.MixerClient, pb.ReconClient, bool),
) error {
	mixer, recon, err := Setup(opt)
	if err != nil {
		return fmt.Errorf("failed to set up mixer and client: %s", err)
	}
	testSuite(mixer, recon, false /* latencyTest */)
	return nil
}

func latencyTest(
	apiName string,
	opt *TestOption,
	testSuite func(pb.MixerClient, pb.ReconClient, bool),
) error {
	durationStore := []float64{}
	mixer, recon, err := Setup(opt)
	if err != nil {
		return fmt.Errorf("failed to set up mixer and client")
	}
	// Run multiple times to reduce fluctuations.
	for i := 0; i < numTestTimes; i++ {
		startTime := time.Now()
		testSuite(mixer, recon, true /* latencyTest */)
		durationStore = append(durationStore, time.Since(startTime).Seconds())
	}
	value := meanValue(durationStore)
	resultCsvRow := fmt.Sprintf("%s,%.3f\n", apiName, value)
	fmt.Println(resultCsvRow)

	_, filename, _, _ := runtime.Caller(0)
	resultFilePath := path.Join(
		path.Dir(filename), "latency", fmt.Sprintf("%s.csv", apiName))
	return os.WriteFile(resultFilePath, []byte(resultCsvRow), 0644)
}

func meanValue(list []float64) float64 {
	res := 0.0
	for _, item := range list {
		res += item
	}
	return res / float64(len(list))
}
