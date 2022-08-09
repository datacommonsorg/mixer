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

package main

import (
	"fmt"
	"os"
	"log"
	"errors"
	"github.com/google/pprof/profile"
)

// sample index of alloc_space in neap profiles from http/net/pprof
const alloc_space_sample_index = 1;
// Modified from https://github.com/google/pprof/blob/c488b8fa1db3fa467bf30beb5a1d6f4f10bb1b87/internal/report/report.go
// =======================
// computeTotal computes the sum of the absolute value of all sample values.
// If any samples have label indicating they belong to the diff base, then the
// total will only include samples with that label.
func computeTotal(prof *profile.Profile, value func(v []int64) int64) int64 {
	var total, diffTotal int64
	for _, sample := range prof.Sample {
		var v int64
		v = value(sample.Value)
		fmt.Println(v)
		if v < 0 {
			v = -v
		}
		total += v
		if sample.DiffBaseSample() {
			diffTotal += v
		}
	}
	if diffTotal > 0 {
		total = diffTotal
	}
	return total
}

func readProfile (filename string) (*profile.Profile, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, errors.New("file could not be opened: " + filename)
	}

	prof, err := profile.Parse(reader)
	if err != nil {
		return nil, fmt.Errorf("could not parse proto file: %v", filename)
	}
	return prof, err
}

func GetTotalSpaceAllocFromProfile(filename string) (int64) {

	prof, err := readProfile(filename)
	if err != nil {
		log.Fatal(err)
	}
	valuef := func(v []int64) int64 {
		return v[alloc_space_sample_index]
	}
	total := computeTotal(prof, valuef)
	return total
}

func BytesToMegabytes(bytes int64) int64 {
	return bytes / (1024 * 1024);
}

// Main is an example usage of the functions defined in this folder
func main() {
	testFile := "http_memprof_out/go_http_memprof.before.BulkObservationsSeriesLinked_USA.pb"

	total := GetTotalSpaceAllocFromProfile(testFile)
	totalMb := BytesToMegabytes(total)

	fmt.Printf("got total %d MB [%d bytes]\n", totalMb, total)

	fmt.Println("exiting prof.go::main")

	return;
}
