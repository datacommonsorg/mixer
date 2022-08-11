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

package main

import (
	"context"
	"flag"
	"log"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"errors"
	"github.com/google/pprof/profile"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc"
)


var (
	grpcAddr = flag.String("grpc_addr", "127.0.0.1:12345", "Address of grpc server.")
	profAddr = flag.String("prof_addr", "http://localhost:6060", "Address of HTTP profile server.")
	tempPath = flag.String("temp_path", "http_memprof_out", "Folder to store temporary output of memory profiles over HTTP")
)

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
	// Modified from https://github.com/google/pprof/blob/c488b8fa1db3fa467bf30beb5a1d6f4f10bb1b87/internal/report/report.go
	// =======================
	// computeTotal computes the sum of the absolute value of all sample values.
	// If any samples have label indicating they belong to the diff base, then the
	// total will only include samples with that label.
	computeTotal := func (prof *profile.Profile, value func(v []int64) int64) int64 {
		var total, diffTotal int64
		for _, sample := range prof.Sample {
			var v int64
			v = value(sample.Value)
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
	// sample index of alloc_space in neap profiles from http/net/pprof
	const allocSpaceSampleIndex = 1;

	prof, err := readProfile(filename)
	if err != nil {
		log.Fatal(err)
	}
	valuef := func(v []int64) int64 {
		return v[allocSpaceSampleIndex]
	}
	total := computeTotal(prof, valuef)
	return total
}

func bytesToMegabytes(bytes int64) int64 {
	return bytes / (1024 * 1024);
}

func saveProfile(outPath string) {
	outputFlag := fmt.Sprintf("-output=%v", outPath)
	heapProfileHttpPath := fmt.Sprintf("%v/debug/pprof/heap?gc=1", *profAddr)
	cmd := exec.Command("go", "tool", "pprof", outputFlag, "-proto", heapProfileHttpPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}
}

func runWithProfile (key string, f func() (string, error)) {
	profileLocationTemplate := *tempPath + "/go_http_memprof.%v.%v.pb"

	profBefore := fmt.Sprintf(profileLocationTemplate, "before", key)
	profAfter := fmt.Sprintf(profileLocationTemplate, "after", key)

	saveProfile(profBefore)
	respStr, err := f()
	if err != nil {
		log.Fatalf("could not run %d: %s", key, err)
	}
	saveProfile(profAfter)

	allocBefore := GetTotalSpaceAllocFromProfile(profBefore)
	allocAfter := GetTotalSpaceAllocFromProfile(profAfter)

	allocDiff := allocAfter - allocBefore
	allocDiffMb := bytesToMegabytes(allocDiff)

	fmt.Printf("%v used %d MB and returned a response of length %d\n", key, allocDiffMb, len(respStr))

	// NOTE: Another approach here would be to use go tool pprof to compute a
	// profile with "substraction", where the profile consists of the
	// differences with the "base profile". The numerical diff can be read from
	// that "diff profile" the same way we read the total memory allocated in
	// the "before" and "after" profiles currently.
	// To achieve this, one could do something like the following:
	//////////
	// profDiff := fmt.Sprintf(profileLocationTemplate, "diff", key)
	// outputFlag := fmt.Sprintf("-output=%v", profDiff)
	// // error-checking omitted from the following
	// cmd := exec.Command("go", "tool", "pprof", "-proto", outputFlag, "-sample_index=alloc_space", "-base", profBefore, profAfter)
	// allocDiffMb := bytesToMegabytes(GetTotalSpaceAllocFromProfile(profDiff)
	//////////
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	// Ensure the temp output path exists
	err := os.MkdirAll(*tempPath, 0o0700)
	if err != nil {
		log.Fatalf("could not create temp directory at %v: %v", tempPath, err)
	}

	// Set up a connection to the server.
	// gRPC server start listening last, so if we have connected to gRPC, we can
	// assume that the pprof HTTP handlers is also on.
	fmt.Printf("Attempting to make a connection to the gRPC server at %v\n", *grpcAddr)
	conn, err := grpc.Dial(*grpcAddr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)),
		// The grpc.WithBlock() option make this call block until a connection is
		// established, internally retrying until needed.
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("Could not connect: %v\n", err)
	}
	log.Println("Connected to gRPC succesfully")

	defer conn.Close()
	c := pb.NewMixerClient(conn)
	ctx := context.Background()

	var count int64
	for _, allFacets := range []bool{true, false} {
		for _, r := range []struct{
			entityType   string
			linkedEntity string
			variables    []string
		}{
				{
					"County",
					"country/USA",
					[]string{"dummy", "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
				},
				{
					"City",
					"country/USA",
					[]string{"Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
				},
				{
					"State",
					"country/USA",
					[]string{"Count_Person_FoodInsecure"},
				},
				{
					"Country",
					"Earth",
					[]string{"Median_Age_Person"},
				},
				{
					"EpaReportingFacility",
					"geoId/06",
					[]string{"Annual_Emissions_GreenhouseGas_NonBiogenic"},
				},
				{
					"AdministrativeArea2",
					"country/FRA",
					[]string{"Count_Person"},
				},
		}{
			funcKey := fmt.Sprintf("BulkObservationsSeriesLinked_%d", count)
			funcToProfile := func() (string, error) {
				req := &pb.BulkObservationsSeriesLinkedRequest{
					Variables:      r.variables,
					EntityType:     r.entityType,
					LinkedEntity:   r.linkedEntity,
					LinkedProperty: "containedInPlace",
					AllFacets:      allFacets,
				}
				resp, err := c.BulkObservationsSeriesLinked(ctx,req)
				if err != nil {
					return "", err
				}
				respStr := resp.String()
				return respStr, nil
			}
			runWithProfile(funcKey, funcToProfile)
			count++
		}
	}

	return;
}
